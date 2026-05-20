# Race Events Kafka Pipeline

Демонстрация работы Apache Kafka с примером гоночных событий (F1, NASCAR, LeMans).

## Архитектура

- **Producer** (FastAPI): Генерирует события о гонках в три отдельных topic-а
- **Kafka Topics**: `f1_events`, `nascar_events`, `lemans_events` (по 2 партиции каждый)
- **Consumer 1**: Читает все три topic-а → выводит события в реальном времени с эмодзи
- **Consumer 2**: Читает все три topic-а → отслеживает лидеров по каждой гонке
- **Kafdrop**: Веб-интерфейс для мониторинга topic-ов и сообщений

## Запуск

### 1. Поднять стек
```bash
chmod +x create_topic.sh
docker compose up -d --build
```

Проверить статус контейнеров:
```bash
docker compose ps
```

### 2. Запустить генерацию событий

Открыть в браузере http://localhost:8000/docs и нажать **Try it out** на эндпоинте `/start_race_events`.

Или в терминале:
```bash
curl -X POST "http://localhost:8000/start_race_events" \
  -H "Content-Type: application/json"
```

### 3. Смотреть логи

**Consumer 1 (Live Updates)**:
```bash
docker compose logs -f consumer1
```

**Consumer 2 (Leaders Tracking)**:
```bash
docker compose logs -f consumer2
```

### 4. Мониторинг в Kafdrop

Открыть http://localhost:9000 и выбрать topic для просмотра сообщений.

## Структура сообщения

```json
{
  "race": "F1",
  "driver": "Hamilton",
  "position": 1,
  "lap": 50,
  "timestamp": "2026-05-20T11:20:16.123456"
}
```

## API Endpoints

- `GET /` - Статус приложения
- `POST /start_race_events` - Начать генерацию событий
- `GET /race_status` - Текущий статус гонок
- `GET /docs` - Swagger UI

## Отключение стека

```bash
docker compose down
```

