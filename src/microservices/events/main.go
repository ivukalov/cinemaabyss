// Events Service (MVP) — проверка гипотезы «насколько просто применить Kafka
// в архитектуре Кинобездны».
//
// Сервис одновременно продюсер и консьюмер:
//   - POST /api/events/movie|user|payment публикует событие в соответствующий
//     топик Kafka;
//   - фоновая горутина читает все три топика и пишет полученные события в лог.
//
// Конфигурация:
//
//	PORT           порт HTTP-сервера (8082)
//	KAFKA_BROKERS  брокеры Kafka через запятую (kafka:9092)
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	topicMovies   = "movie-events"
	topicUsers    = "user-events"
	topicPayments = "payment-events"

	consumerGroup = "events-service"
	maxBodyBytes  = 1 << 20 // 1 MiB
)

// Event — конверт, в котором событие уходит в Kafka.
type Event struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Topic       string          `json:"topic"`
	PublishedAt time.Time       `json:"published_at"`
	Payload     json.RawMessage `json:"payload"`
}

// publisher держит по одному writer'у на топик.
type publisher struct {
	writers map[string]*kafka.Writer
	ready   atomic.Bool
}

func newPublisher(brokers []string) *publisher {
	writers := make(map[string]*kafka.Writer, 3)
	for _, topic := range []string{topicMovies, topicUsers, topicPayments} {
		writers[topic] = &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  topic,
			Balancer:               &kafka.LeastBytes{},
			RequiredAcks:           kafka.RequireOne,
			MaxAttempts:            10,
			AllowAutoTopicCreation: true,
			BatchTimeout:           10 * time.Millisecond,
			WriteTimeout:           10 * time.Second,
		}
	}
	return &publisher{writers: writers}
}

func (p *publisher) publish(ctx context.Context, topic, eventType string, payload json.RawMessage) error {
	event := Event{
		ID:          newID(),
		Type:        eventType,
		Topic:       topic,
		PublishedAt: time.Now().UTC(),
		Payload:     payload,
	}
	value, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("сериализация события: %w", err)
	}

	w, ok := p.writers[topic]
	if !ok {
		return fmt.Errorf("неизвестный топик %q", topic)
	}
	if err := w.WriteMessages(ctx, kafka.Message{Value: value}); err != nil {
		return fmt.Errorf("запись в %s: %w", topic, err)
	}

	p.ready.Store(true)
	log.Printf("[producer] topic=%s type=%s id=%s", topic, eventType, event.ID)
	return nil
}

func (p *publisher) close() {
	for _, w := range p.writers {
		_ = w.Close()
	}
}

// consume читает все топики и пишет события в лог.
func consume(ctx context.Context, brokers []string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		GroupID:     consumerGroup,
		GroupTopics: []string{topicMovies, topicUsers, topicPayments},
		MinBytes:    1,
		MaxBytes:    10 << 20,
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	log.Printf("[consumer] подписан на %s, %s, %s (group=%s)",
		topicMovies, topicUsers, topicPayments, consumerGroup)

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("[consumer] остановлен")
				return
			}
			log.Printf("[consumer] ошибка чтения: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}
		log.Printf("[consumer] topic=%s partition=%d offset=%d value=%s",
			msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
	}
}

func main() {
	brokers := strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ",")
	pub := newPublisher(brokers)
	defer pub.close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go consume(ctx, brokers)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/events/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"status": true,
			"kafka":  pub.ready.Load(),
		})
	})
	mux.HandleFunc("/api/events/movie", eventHandler(pub, topicMovies, "movie"))
	mux.HandleFunc("/api/events/user", eventHandler(pub, topicUsers, "user"))
	mux.HandleFunc("/api/events/payment", eventHandler(pub, topicPayments, "payment"))

	srv := &http.Server{
		Addr:              ":" + env("PORT", "8082"),
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("[events] слушаю порт %s, брокеры: %v", srv.Addr, brokers)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("[events] сервер остановлен: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("[events] ошибка остановки: %v", err)
	}
	log.Println("[events] остановлен")
}

// eventHandler публикует тело запроса в топик и отвечает 201.
func eventHandler(pub *publisher, topic, eventType string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}

		body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "не удалось прочитать тело"})
			return
		}
		body = []byte(strings.TrimSpace(string(body)))
		if len(body) == 0 || !json.Valid(body) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "тело должно быть JSON"})
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()

		if err := pub.publish(ctx, topic, eventType, body); err != nil {
			log.Printf("[events] не удалось опубликовать %s: %v", eventType, err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}

		writeJSON(w, http.StatusCreated, map[string]string{"status": "success"})
	}
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

// newID возвращает случайный 32-символьный идентификатор события.
func newID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
