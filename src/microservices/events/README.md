# Events Service (MVP)

MVP шины событий: публикует событие по вызову API и сам его читает, записывая в лог.

## API

| Метод | Путь | Топик | Ответ |
|---|---|---|---|
| `GET` | `/api/events/health` | — | `200 {"status":true,"kafka":bool}` |
| `POST` | `/api/events/movie` | `movie-events` | `201 {"status":"success"}` |
| `POST` | `/api/events/user` | `user-events` | `201 {"status":"success"}` |
| `POST` | `/api/events/payment` | `payment-events` | `201 {"status":"success"}` |

Тело запроса — произвольный JSON-объект события. Он заворачивается в конверт:

```json
{
  "id": "6d643a651c4a97c83fe8fc88be007410",
  "type": "movie",
  "topic": "movie-events",
  "published_at": "2026-11-21T15:34:55.652129215Z",
  "payload": {"movie_id": 1, "title": "Test", "action": "viewed", "user_id": 1}
}
```

## Конфигурация

| Переменная | По умолчанию | Назначение |
|---|---|---|
| `PORT` | `8082` | Порт HTTP-сервера |
| `KAFKA_BROKERS` | `localhost:9092` | Брокеры через запятую |

## Логи

```
[producer] topic=movie-events type=movie id=6d643a65...
[consumer] topic=movie-events partition=0 offset=1 value={"id":"6d643a65...","payload":{...}}
```

## Запуск

```bash
docker-compose up -d --build events-service
curl -X POST http://localhost:8082/api/events/movie \
  -H 'Content-Type: application/json' \
  -d '{"movie_id":1,"title":"Test","action":"viewed","user_id":1}'
docker-compose logs -f events-service
```

Топики и сообщения видны в Kafka UI: <http://localhost:8090>.
