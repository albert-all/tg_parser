# Инструкция по развёртыванию Telegram бота на Ubuntu сервере

## Содержание
1. [Подготовка сервера](#подготовка-сервера)
2. [Установка Docker](#установка-docker)
3. [Подготовка проекта](#подготовка-проекта)
4. [Развёртывание приложения](#развёртывание-приложения)
5. [Проверка и мониторинг](#проверка-и-мониторинг)
6. [Обновление проекта](#обновление-проекта)

---

## Подготовка сервера

### Шаг 1: Обновление системы
```bash
sudo apt update
sudo apt upgrade -y
sudo apt install -y curl wget git nano
```

### Шаг 2: Создание пользователя для приложения (опционально)
```bash
sudo useradd -m -s /bin/bash tg_bot
sudo usermod -aG docker tg_bot
```

### Шаг 3: Выбор директории для проекта
```bash
cd /opt
sudo mkdir -p tg_parser
sudo chown $USER:$USER /opt/tg_parser  # Замените если используете другого пользователя
cd /opt/tg_parser
```

---

## Установка Docker

### Шаг 1: Удалить старые версии Docker (если есть)
```bash
sudo apt remove -y docker docker-engine docker.io containerd runc
```

### Шаг 2: Установить Docker с официального репозитория
```bash
sudo apt install -y apt-transport-https ca-certificates curl software-properties-common

curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -

sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"

sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
```

### Шаг 3: Проверка установки Docker
```bash
docker --version
docker compose version
```

### Шаг 4: Запуск Docker služby (сервиса)
```bash
sudo systemctl enable docker
sudo systemctl start docker
```

### Шаг 5: Добавление текущего пользователя в группу docker (опционально)
```bash
sudo usermod -aG docker $USER
newgrp docker
# Выйдите и войдите снова, чтобы изменения вступили в силу
```

---

## Подготовка проекта

### Шаг 1: Клонирование репозитория на сервер

**Способ 1: Через Git**
```bash
cd /opt/tg_parser
git clone https://github.com/ваш-аккаунт/тг_parser.git .
```

**Способ 2: Скопирование через SCP (с локальной машины)**
```bash
# С вашей локальной машины:
scp -r ./tg_parser user@server_ip:/opt/tg_parser
```

**Способ 3: Скопирование файлов вручную**
```bash
# На сервере
cd /opt/tg_parser
# Скопируйте все файлы проекта
```

### Шаг 2: Создание файла .env с переменными окружения
```bash
cd /opt/tg_parser
nano .env
```

Содержимое `.env`:
```env
# Telegram Bot Configuration
BOT_TOKEN=ваш_токен_бота_от_botfather
TG_API_ID=ваш_api_id_от_telegram
TG_API_HASH=ваш_api_hash_от_telegram

# Database Configuration
POSTGRES_DB=tg_bot
POSTGRES_USER=tg_bot
POSTGRES_PASSWORD=ПРИДУМАЙТЕ_СИЛЬНЫЙ_ПАРОЛЬ_12345

# Bot Configuration
RETENTION_DAYS=30
DEFAULT_LIMIT=50
QR_TIMEOUT_SECONDS=180
```

⚠️ **Важно:** 
- Используйте сильный пароль для базы данных (минимум 16 символов)
- BOT_TOKEN получите у @BotFather в Telegram
- TG_API_ID и TG_API_HASH получите на https://my.telegram.org

### Шаг 3: Проверка файловой структуры
```bash
  ls -la /opt/tg_parser/
# Должны быть: docker-compose.yml, Dockerfile.bot, requirements_bot.txt, tg_bot.py, .env
```

---

## Развёртывание приложения

### Шаг 1: Сборка и запуск контейнеров
```bash
cd /opt/tg_parser
docker compose up -d --build
```

Эта команда:
- `-d` запускает в фоновом режиме
- `--build` пересобирает образы

### Шаг 2: Проверка статуса контейнеров
```bash
docker compose ps
```

Вы должны увидеть:
```
NAME      IMAGE              COMMAND              SERVICE   STATUS
bot       tg_parser_bot      python tg_bot.py     bot       Up X seconds
db        postgres:16-alpine postgres             db        Up X seconds
```

### Шаг 3: Проверка логов
```bash
# Логи всех сервисов
docker compose logs -f

# Логи только бота
docker compose logs -f bot

# Логи только БД
docker compose logs -f db
```

---

## Проверка и мониторинг

### Шаг 1: Убедитесь, что бот работает
```bash
# Отправьте команду боту в Telegram и проверьте логи
docker compose logs -f bot | grep -i "success\|error\|started"
```

### Шаг 2: Проверка базы данных
```bash
docker compose exec db psql -U tg_bot -d tg_bot -c "\dt"
```

### Шаг 3: Создание systemd сервиса для автозапуска (рекомендуется)

Создайте файл:
```bash
sudo nano /etc/systemd/system/tg-bot.service
```

Содержимое:
```ini
[Unit]
Description=Telegram Bot Service
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/tg_parser
ExecStart=/usr/bin/docker compose up
ExecStop=/usr/bin/docker compose down
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Включите сервис:
```bash
sudo systemctl daemon-reload
sudo systemctl enable tg-bot.service
sudo systemctl start tg-bot.service

# Проверка статуса
sudo systemctl status tg-bot.service
```

### Шаг 4: Просмотр логов через journalctl
```bash
sudo journalctl -u tg-bot.service -f
```

---

## Обновление проекта

### При изменении кода:
```bash
cd /opt/tg_parser

# Получить последние изменения
git pull origin main

# Пересобрать и перезапустить
docker compose down
docker compose up -d --build

# Проверить логи
docker compose logs -f
```

### При изменении переменных окружения:
```bash
cd /opt/tg_parser

# Отредактируйте .env
nano .env

# Перезапустите контейнеры
docker compose restart
```

### Резервное копирование БД:
```bash
# Создать резервную копию
docker compose exec db pg_dump -U tg_bot tg_bot > backup_$(date +%Y%m%d_%H%M%S).sql

# Восстановить из резервной копии
docker compose exec -T db psql -U tg_bot tg_bot < backup_20240305_120000.sql
```

---

## Очистка и удаление

### Остановить все контейнеры:
```bash
cd /opt/tg_parser
docker compose down
```

### Удалить все данные (ОСТОРОЖНО!):
```bash
docker compose down -v  # -v удалит volumes (базу данных)
```

### Удалить неиспользуемые Docker ресурсы:
```bash
docker system prune -a
```

---

## Диагностика проблем

### Проблема: Контейнер не запускается
```bash
# Посмотрите логи
docker compose logs bot

# Проверьте файл .env
cat .env | grep -v "^#"

# Пересоберите образ
docker compose build --no-cache
docker compose up -d
```

### Проблема: БД недоступна
```bash
# Проверьте что БД запущена
docker compose ps db

# Проверьте здоровье БД
docker compose ps

# Если статус unhealthy, перезагрузите:
docker compose restart db
```

### Проблема: Нет интернета в контейнере
```bash
# Проверьте DNS
docker compose exec bot cat /etc/resolv.conf

# Перезагрузите сетевой интерфейс Docker
sudo systemctl restart docker
```

### Просмотр всех ошибок
```bash
docker compose logs --tail=100
```

---

## Мониторинг и поддержка

### Установка инструмента для мониторинга (опционально):
```bash
# Установка Portainer для веб-интерфейса управления Docker
docker run -d -p 8000:8000 -p 9000:9000 \
  --name=portainer --restart=always \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v portainer_data:/data \
  portainer/portainer-ce
```

Доступ: `http://ваш_ip:9000`

---

## Чек-лист для проверки

- [ ] Docker и docker compose установлены
- [ ] Проект скопирован в `/opt/tg_parser`
- [ ] Файл `.env` создан и заполнен
- [ ] Контейнеры успешно запущены: `docker compose ps`
- [ ] Бот получает сообщения от Telegram
- [ ] БД инициализирована и доступна
- [ ] Настроен автозапуск через systemd
- [ ] Созданы резервные копии данных
- [ ] Настроен мониторинг логов

---

## Полезные команды

```bash
# Перезагрузить все контейнеры
docker compose restart

# Просмотр ресурсов, используемых контейнерами
docker stats

# Удаление остановленных контейнеров
docker container prune

# Просмотр последних 50 строк логов
docker compose logs --tail=50

# Вход в контейнер бота
docker compose exec bot bash

# Вход в базу данных
docker compose exec db psql -U tg_bot -d tg_bot
```

---

## Заключение

Ваш Telegram-бот теперь полностью развёрнут на Ubuntu сервере! 

Для дополнительной помощи:
- Проверьте логи: `docker compose logs -f`
- Убедитесь, что переменные окружения корректны
- Проверьте подключение к интернету на сервере
