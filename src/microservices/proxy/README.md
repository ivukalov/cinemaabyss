# Proxy Service (API Gateway)

Единая точка входа для клиентов и рубильник миграции по паттерну **Strangler Fig**:
принимает запрос и решает, в какой апстрим его отправить — в монолит или в уже
вынесенный микросервис.

## Конфигурация

| Переменная | По умолчанию | Назначение |
|---|---|---|
| `PORT` | `8000` | Порт, который слушает прокси |
| `MONOLITH_URL` | `http://localhost:8080` | Адрес монолита |
| `MOVIES_SERVICE_URL` | `http://localhost:8081` | Адрес сервиса фильмов |
| `EVENTS_SERVICE_URL` | `http://localhost:8082` | Адрес сервиса событий |
| `GRADUAL_MIGRATION` | `false` | `true` — включить процентную маршрутизацию |
| `MOVIES_MIGRATION_PERCENT` | `0` | Доля трафика `/api/movies` (0–100) в новый сервис |

## Маршрутизация

| Маршрут | Апстрим |
|---|---|
| `/health` | Сам прокси, `{"status":true}` |
| `/api/movies/health` | Всегда `movies-service` (предсказуемый health-check) |
| `/api/movies` | `movies-service` или монолит по флагу |
| `/api/events/*` | `events-service` |
| `/api/users`, `/api/payments`, `/api/subscriptions` | Монолит |
| всё остальное | Монолит |

Семантика флага:

- `GRADUAL_MIGRATION=false` — домен считается полностью переехавшим, весь трафик
  идёт в микросервис;
- `GRADUAL_MIGRATION=true` — `MOVIES_MIGRATION_PERCENT`% запросов уходит в
  микросервис, остаток в монолит.

Откат любого этапа миграции — вернуть процент в `0`, без релиза и пересборки.
Каждое решение о маршруте пишется в лог:

```
[proxy] GET /api/movies -> movies-service (доля трафика на новый сервис)
[proxy] GET /api/movies -> monolith (остаток трафика в монолит)
```

## Запуск

В составе стека:

```bash
docker-compose up -d --build proxy-service
curl http://localhost:8000/api/movies
```

Локально:

```bash
cd src/microservices/proxy
MONOLITH_URL=http://localhost:8080 MOVIES_SERVICE_URL=http://localhost:8081 go run .
```
