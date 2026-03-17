# Краткая шпаргалка по развёртыванию

## Первый запуск (за 5 минут)

```bash
# 1. Установка Docker (на Ubuntu)
sudo apt install -y docker.io docker-compose-plugin
sudo apt update && sudo apt upgrade -y

# 2. Клонирование проекта
cd /opt
sudo mkdir tg_parser && cd tg_parser
git clone <URL вашего репозитория> .

# 3. Создание .env файла
cat > .env << EOF
BOT_TOKEN=ваш_токен
TG_API_ID=ваш_api_id
TG_API_HASH=ваш_api_hash
POSTGRES_PASSWORD=сильный_пароль
RETENTION_DAYS=30
DEFAULT_LIMIT=50
QR_TIMEOUT_SECONDS=180
EOF

# 4. Запуск
docker compose up -d --build

# 5. Проверка
docker compose ps
docker compose logs -f bot
```

## Ежедневные команды

```bash
# Посмотреть статус
docker compose ps

# Логи бота
docker compose logs -f bot

# Перезагрузить бота
docker compose restart bot

# Остановить всё
docker compose down

# Запустить всё
docker compose up -d
```

## Диагностика

```bash
# Проверить ошибки
docker compose logs bot | tail -20

# Статус БД
docker compose exec db psql -U tg_bot -d tg_bot -c "SELECT version();"

# Контроль ресурсов
docker stats

# Вход в контейнер
docker compose exec bot bash
```

## Обновление проекта

```bash
cd /opt/tg_parser
git pull
docker compose down
docker compose up -d --build
docker compose logs -f bot
```

## Резервная копия БД

```bash
# Создать
docker compose exec db pg_dump -U tg_bot tg_bot > backup.sql

# Восстановить
docker compose exec -T db psql -U tg_bot tg_bot < backup.sql
```

## Автозапуск при перезагрузке сервера

```bash
# Создать service
sudo nano /etc/systemd/system/tg-bot.service

# Вставить:
# [Unit]
# Description=Telegram Bot Service
# After=network.target
# [Service]
# Type=simple
# User=root
# WorkingDirectory=/opt/tg_parser
# ExecStart=/usr/bin/docker compose up
# ExecStop=/usr/bin/docker compose down
# Restart=always
# [Install]
# WantedBy=multi-user.target

# Включить
sudo systemctl daemon-reload
sudo systemctl enable tg-bot.service
sudo systemctl start tg-bot.service
sudo systemctl status tg-bot.service
```

## Важные переменные .env

| Переменная | Описание | Где взять |
|---|---|---|
| `BOT_TOKEN` | Токен бота | @BotFather в Telegram |
| `TG_API_ID` | API ID | my.telegram.org |
| `TG_API_HASH` | API Hash | my.telegram.org |
| `POSTGRES_PASSWORD` | Пароль БД | Сами придумайте (сильный) |
| `RETENTION_DAYS` | Дней хранения | По умолчанию 30 |
| `DEFAULT_LIMIT` | Лимит по умолчанию | По умолчанию 50 |

## Порты

- **БД PostgreSQL**: 5432 (внутренний, не доступен снаружи)
- **Бот**: Использует API Telegram (не нужны открытые порты)

## Если что-то не работает

1. Проверить логи: `docker compose logs -f`
2. Проверить `.env`: `cat .env`
3. Перезагрузить: `docker compose restart`
4. Пересобрать: `docker compose up -d --build`
5. Проверить интернет: `docker compose exec bot ping google.com`
