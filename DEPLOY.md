# Инструкция по деплою в Yandex Cloud Serverless Containers

## 🚀 Быстрый старт с GitHub Actions (рекомендуется)

Если вы хотите автоматический деплой без установки Docker локально, используйте GitHub Actions:

1. **Настройте секреты в GitHub** (см. раздел ниже)
2. **Сделайте push в main** - образ соберется и загрузится автоматически
3. **Создайте Serverless Container** с образом из Container Registry

Подробная инструкция: [.github/workflows/README.md](.github/workflows/README.md)

---

## Подготовка (для локальной сборки)

## Настройка GitHub Actions

### 1. Создать Service Account в Yandex Cloud

1. Yandex Cloud Console → **IAM** → **Service Accounts**
2. Нажмите **Создать сервисный аккаунт**
3. Укажите имя (например, `github-actions-bot`)
4. Назначьте роль: **Container Registry → Images Pusher** (`container-registry.images.pusher`)
5. Создайте ключ:
   - Service Account → **Keys** → **Create key** → **JSON**
   - Скопируйте весь JSON (минифицированный, в одну строку)

### 2. Добавить секреты в GitHub

1. Откройте репозиторий на GitHub
2. **Settings** → **Secrets and variables** → **Actions** → **New repository secret**
3. Добавьте секреты:

   - **`YC_REGISTRY_ID`** — ID вашего Container Registry
     - Где найти: Container Registry → ваш registry → ID
     - Пример: `crp1234567890abcdef`
   
   - **`YC_SA_JSON_CREDENTIALS`** — JSON ключ Service Account
     - Вставьте весь JSON ключ (минифицированный, в одну строку)

### 3. Создать Container Registry

1. Yandex Cloud Console → **Container Registry**
2. Нажмите **Создать registry**
3. Укажите имя (например, `office-schedule-bot-registry`)
4. Запомните **Registry ID**

### 4. Запустить workflow

- **Автоматически:** Сделайте push в ветку `main`
- **Вручную:** GitHub → Actions → Build and Push to Yandex Cloud → Run workflow

После успешного выполнения используйте образ:
```
cr.yandex/<YC_REGISTRY_ID>/office-schedule-bot:latest
```

---

## Локальная сборка (альтернатива)

### 1. Установить Docker Desktop

**macOS:**
1. Скачать: https://www.docker.com/products/docker-desktop/
2. Установить и запустить Docker Desktop
3. Дождаться полного запуска (иконка в строке меню должна быть зеленая)

**Проверка:**
```bash
docker --version
```

### 2. Установить Yandex Cloud CLI (опционально, но рекомендуется)

**macOS:**
```bash
brew install yandex-cloud-cli
# или
curl -sSL https://storage.yandexcloud.net/yandexcloud-yc/install.sh | bash
```

**Инициализация:**
```bash
yc init
```

**Проверка:**
```bash
yc --version
```

## Создание Container Registry в Yandex Cloud

1. Откройте [Yandex Cloud Console](https://console.cloud.yandex.ru/)
2. Перейдите в **Container Registry**
3. Нажмите **Создать registry**
4. Укажите имя (например, `office-schedule-bot-registry`)
5. Запомните **Registry ID** (например, `crp1234567890abcdef`)

## Сборка и загрузка образа

### Вариант 1: Автоматический скрипт (рекомендуется)

```bash
cd /Users/rsidorenkov1/office_schedule_bot

# Запустить скрипт
./build_and_push.sh

# При запросе ввести Registry ID
```

### Вариант 2: Вручную

#### Если установлен yc CLI:

```bash
cd /Users/rsidorenkov1/office_schedule_bot

# 1. Собрать образ
docker build -t office-schedule-bot:latest .

# 2. Настроить авторизацию
yc container registry configure-docker

# 3. Тегировать образ (замените <registry-id> на ваш)
docker tag office-schedule-bot:latest cr.yandex/<registry-id>/office-schedule-bot:latest

# 4. Загрузить образ
docker push cr.yandex/<registry-id>/office-schedule-bot:latest
```

#### Если yc CLI не установлен:

```bash
cd /Users/rsidorenkov1/office_schedule_bot

# 1. Собрать образ
docker build -t office-schedule-bot:latest .

# 2. Сохранить образ в файл
docker save office-schedule-bot:latest | gzip > office-schedule-bot.tar.gz

# 3. Загрузить через веб-интерфейс:
#    - Container Registry → ваш registry → Images → Upload
#    - Выбрать файл office-schedule-bot.tar.gz
```

## Создание Serverless Container

1. Откройте **Serverless Containers** в Yandex Cloud Console
2. Нажмите **Создать контейнер**
3. Заполните форму:

   **Основные параметры:**
   - **Имя:** `office-schedule-bot`
   - **Docker-образ:** `cr.yandex/<registry-id>/office-schedule-bot:latest`
   
   **Ресурсы:**
   - **Память:** 1 GB (минимум 512 MB)
   - **CPU:** 1 vCPU (минимум 0.5)
   
   **Переменные окружения:**
   - `BOT_TOKEN` = ваш токен бота
   - `ADMIN_IDS` = `312551109` (или список через запятую)
   - `GOOGLE_SHEETS_ID` = `13zmdoS160B5Hn0Cl-q2hNrEgyZVc6Jh0JaxUnI9jSFg`
   - `GOOGLE_SHEETS_CREDENTIALS` = минифицированный JSON (в одну строку)
   - `USE_GOOGLE_SHEETS` = `true`
   - `GOOGLE_CREDENTIALS_FILE` = `google_credentials.json` (опционально)

   **Дополнительно:**
   - **Таймаут:** 300 секунд
   - **Режим:** "Всегда запущен" (если доступен)

4. Нажмите **Создать**

## Проверка работы

1. После создания контейнера проверьте логи
2. Отправьте боту команду `/start` в Telegram
3. Проверьте, что бот отвечает

## Обновление образа

### Автоматически (через скрипт):
```bash
./build_and_push.sh
```

### Вручную:
```bash
# Пересобрать образ
docker build -t office-schedule-bot:latest .

# Загрузить (если используете yc)
docker tag office-schedule-bot:latest cr.yandex/<registry-id>/office-schedule-bot:latest
docker push cr.yandex/<registry-id>/office-schedule-bot:latest

# Перезапустить контейнер в Yandex Cloud Console
```

## Troubleshooting

### Docker не запускается
- Проверьте, что Docker Desktop запущен
- Перезапустите Docker Desktop

### Ошибка авторизации в Container Registry
```bash
yc container registry configure-docker
```

### Ошибка при сборке образа
- Проверьте, что все файлы на месте
- Проверьте `requirements.txt`
- Попробуйте собрать с очисткой кэша: `docker build --no-cache -t office-schedule-bot .`

### Бот не запускается в контейнере
- Проверьте логи в Yandex Cloud Console
- Проверьте переменные окружения
- Убедитесь, что `BOT_TOKEN` указан правильно

