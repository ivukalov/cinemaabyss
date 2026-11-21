// Proxy Service — API Gateway для постепенной миграции с монолита на сервисы
// (паттерн Strangler Fig).
//
// Сервис принимает весь клиентский трафик и решает, куда его отправить:
//   - домен, который уже вынесен в микросервис, — в этот сервис;
//   - домен, который ещё живёт в монолите, — в монолит;
//   - домен в процессе переезда — доля трафика (MOVIES_MIGRATION_PERCENT)
//     уходит в микросервис, остаток — в монолит.
//
// Переключение управляется переменными окружения, без пересборки и релиза:
//
//	PORT                      порт, который слушает прокси (8000)
//	MONOLITH_URL              адрес монолита
//	MOVIES_SERVICE_URL        адрес сервиса фильмов
//	EVENTS_SERVICE_URL        адрес сервиса событий
//	GRADUAL_MIGRATION         "true" — включить процентную маршрутизацию
//	MOVIES_MIGRATION_PERCENT  процент трафика /api/movies в новый сервис (0..100)
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math/rand/v2"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// backend — обёртка над ReverseProxy одного апстрима.
type backend struct {
	name  string
	proxy *httputil.ReverseProxy
}

func newBackend(rawURL, name string) *backend {
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" {
		log.Fatalf("proxy: некорректный адрес %q для %s: %v", rawURL, name, err)
	}

	rp := httputil.NewSingleHostReverseProxy(u)
	base := rp.Director
	rp.Director = func(req *http.Request) {
		base(req)
		req.Host = u.Host
	}
	rp.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("[proxy] %s %s -> %s: ошибка апстрима: %v", r.Method, r.URL.RequestURI(), name, err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": "upstream unavailable",
			"route": name,
		})
	}

	log.Printf("[proxy] %s -> %s", name, u.String())
	return &backend{name: name, proxy: rp}
}

// config — настройки маршрутизации, прочитанные из окружения.
type config struct {
	gradualMigration bool
	migrationPercent int
}

func loadConfig() config {
	percent, err := strconv.Atoi(env("MOVIES_MIGRATION_PERCENT", "0"))
	if err != nil {
		log.Printf("[proxy] MOVIES_MIGRATION_PERCENT=%q не число, считаю 0", os.Getenv("MOVIES_MIGRATION_PERCENT"))
		percent = 0
	}
	if percent < 0 {
		percent = 0
	}
	if percent > 100 {
		percent = 100
	}

	cfg := config{
		gradualMigration: strings.EqualFold(env("GRADUAL_MIGRATION", "false"), "true"),
		migrationPercent: percent,
	}
	log.Printf("[proxy] постепенная миграция: %v, доля трафика в movies-service: %d%%",
		cfg.gradualMigration, cfg.migrationPercent)
	return cfg
}

// route выбирает апстрим для запроса.
func (c config) route(path string, monolith, movies, events *backend) (*backend, string) {
	switch {
	case strings.HasPrefix(path, "/api/events"):
		return events, "домен событий вынесен"

	case strings.HasPrefix(path, "/api/movies/health"):
		// Health-check должен быть предсказуемым — всегда в новый сервис.
		return movies, "health-check сервиса фильмов"

	case strings.HasPrefix(path, "/api/movies"):
		if !c.gradualMigration {
			// Миграция завершена: весь домен фильмов в новом сервисе.
			return movies, "миграция завершена"
		}
		if c.migrationPercent >= 100 {
			return movies, "100% на новый сервис"
		}
		if rand.IntN(100) < c.migrationPercent {
			return movies, "доля трафика на новый сервис"
		}
		return monolith, "остаток трафика в монолит"

	default:
		// users, payments, subscriptions и всё прочее пока в монолите.
		return monolith, "домен ещё в монолите"
	}
}

func main() {
	monolith := newBackend(env("MONOLITH_URL", "http://localhost:8080"), "monolith")
	movies := newBackend(env("MOVIES_SERVICE_URL", "http://localhost:8081"), "movies-service")
	events := newBackend(env("EVENTS_SERVICE_URL", "http://localhost:8082"), "events-service")

	cfg := loadConfig()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		target, reason := cfg.route(r.URL.Path, monolith, movies, events)
		log.Printf("[proxy] %s %s -> %s (%s)", r.Method, r.URL.RequestURI(), target.name, reason)
		target.proxy.ServeHTTP(w, r)
	})

	srv := &http.Server{
		Addr:              ":" + env("PORT", "8000"),
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("[proxy] слушаю порт %s", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("[proxy] сервер остановлен: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("[proxy] ошибка остановки: %v", err)
	}
	log.Println("[proxy] остановлен")
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
