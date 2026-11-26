# Go Backup Service

Учебный backend-сервис для создания архивов каталогов по запросу. HTTP API ставит
задание в очередь PostgreSQL; отдельный worker собирает `tar.gz` и сохраняет
результат на локальный диск.

## Возможности

- REST API на стандартном `net/http`: создать, получить, перечислить и отменить
  задание, скачать готовый архив;
- защита от повторной постановки через заголовок `Idempotency-Key`;
- выбор задания через `FOR UPDATE SKIP LOCKED`, lease с продлением и повторные
  попытки после ошибки или остановки worker;
- версионированные SQL-миграции, индексы для очереди и списка;
- ограничения путей: источник внутри заданного каталога, символические ссылки
  не архивируются, временный файл удаляется при ошибке;
- модульные тесты и интеграционный тест HTTP → PostgreSQL → worker → архив.

Сервис рассчитан на локальный запуск и изучение Go. API не содержит
аутентификации и по умолчанию слушает только `127.0.0.1`; перед публикацией в
сети необходимы аутентификация, TLS и ограничение доступа к данным. Архив
снимается с живого каталога без snapshot файловой системы, поэтому параллельное
изменение файлов может дать несогласованный результат.

## Быстрый запуск

Требуются Go 1.27+, PostgreSQL 16+ или Docker с Compose.

1. Создайте пароль PostgreSQL локально и запустите БД:

   ```bash
   export POSTGRES_PASSWORD='<локальный пароль>'
   docker compose up -d --wait db
   ```

2. Укажите строку подключения и каталоги:

   ```bash
   export DATABASE_URL="postgres://backup_service:${POSTGRES_PASSWORD}@127.0.0.1:5434/backup_service?sslmode=disable"
   export BACKUP_SOURCE_ROOT="$PWD/example-data"
   export BACKUP_OUTPUT_ROOT="$PWD/example-archives"
   mkdir -p "$BACKUP_SOURCE_ROOT/demo"
   printf 'Пример\n' > "$BACKUP_SOURCE_ROOT/demo/readme.txt"
   go run ./cmd/server
   ```

   Сервис сам применяет SQL-миграции при запуске. `BACKUP_OUTPUT_ROOT` должен
   находиться вне `BACKUP_SOURCE_ROOT`.

3. Создайте задание и получите его ID:

   ```bash
   curl -i -X POST http://127.0.0.1:8080/v1/backups \
     -H 'Content-Type: application/json' \
     -H 'Idempotency-Key: demo-1' \
     -d '{"source":"demo"}'
   ```

   ```bash
   curl http://127.0.0.1:8080/v1/backups/<id>
   curl -OJ http://127.0.0.1:8080/v1/backups/<id>/archive
   ```

Пути в `source` задаются относительно `BACKUP_SOURCE_ROOT`. Запрос
`POST /v1/backups/{id}/cancel` отменяет ожидающее или выполняющееся задание.
При отмене во время упаковки worker прекращает работу после следующей проверки
контекста.

## API

| Метод | Путь | Описание |
|---|---|---|
| POST | `/v1/backups` | Создать задание, тело `{"source":"demo"}` |
| GET | `/v1/backups?limit=20` | Последние задания, `limit` от 1 до 100 |
| GET | `/v1/backups/{id}` | Состояние задания |
| POST | `/v1/backups/{id}/cancel` | Отменить задание |
| GET | `/v1/backups/{id}/archive` | Скачать готовый `tar.gz` |
| GET | `/health/live`, `/health/ready` | Состояние процесса и БД |

Контракт описан в [OpenAPI](docs/openapi.yaml). Состояния:
`queued → running → completed`; при ошибке `running → queued` с задержкой,
после третьей попытки — `failed`. Отмена переводит `queued` или `running`
в `cancelled`. При потере lease задание снова доступно worker.

## Проверка

```bash
go test -race ./...
go vet ./...
```

Для интеграционных тестов нужна отдельная тестовая БД. Тесты создают и удаляют
изолированную схему:

```bash
export TEST_DATABASE_URL='postgres://user:password@127.0.0.1:5432/backup_test?sslmode=disable'
go test -race ./... -count=1
```

Без `TEST_DATABASE_URL` тесты, требующие PostgreSQL, пропускаются.

## Структура

- `cmd/server` — запуск HTTP-сервера, БД и worker;
- `internal/api` — HTTP-обработчики и проверка входных данных;
- `internal/store` — SQL-операции и миграции;
- `internal/worker` — обработка очереди и продление lease;
- `internal/backup` — проверка путей и создание архива.
