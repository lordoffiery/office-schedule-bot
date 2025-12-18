# ✅ Проверка загруженного Docker образа

## 🎉 Билд успешно завершен!

Если в GitHub Actions появилась зеленая галочка ✅, значит:
- ✅ Docker образ успешно собран
- ✅ Образ загружен в Yandex Container Registry
- ✅ Образ доступен для использования в Serverless Container

## 🔍 Как проверить образ

### Способ 1: Через Yandex Cloud Console (рекомендуется)

1. Откройте [Yandex Cloud Console](https://console.cloud.yandex.ru/)
2. Перейдите в **Container Registry**
3. Выберите ваш registry: `crpvi47750ad2ea2hfdb`
4. Перейдите на вкладку **Docker-образы**
5. Вы должны увидеть образ: `office-schedule-bot`
6. Нажмите на образ, чтобы увидеть теги:
   - `latest` - последняя версия
   - `[commit-sha]` - версия с хешем коммита

### Способ 2: Через Yandex Cloud CLI (если установлен)

```bash
# Авторизация (если еще не авторизованы)
yc container registry configure-docker --registry-id crpvi47750ad2ea2hfdb

# Проверка образов
yc container image list --registry-id crpvi47750ad2ea2hfdb

# Проверка тегов конкретного образа
yc container image list --registry-id crpvi47750ad2ea2hfdb --folder-name office-schedule-bot
```

### Способ 3: Через Docker CLI

```bash
# Авторизация в Yandex Container Registry
echo "<ваш_json_ключ>" | docker login --username json_key --password-stdin cr.yandex

# Проверка доступности образа
docker pull cr.yandex/crpvi47750ad2ea2hfdb/office-schedule-bot:latest
```

## 📋 Путь к образу для Serverless Container

После успешной загрузки образ доступен по адресу:

```
cr.yandex/crpvi47750ad2ea2hfdb/office-schedule-bot:latest
```

Или с конкретным тегом (хеш коммита):
```
cr.yandex/crpvi47750ad2ea2hfdb/office-schedule-bot:[commit-sha]
```

## 🚀 Следующий шаг: Создать Serverless Container

Теперь можно создать Serverless Container в Yandex Cloud:

1. Yandex Cloud Console → **Serverless Containers**
2. Нажмите **Создать контейнер**
3. Укажите имя (например, `office-schedule-bot`)
4. В поле **Docker-образ** вставьте: `cr.yandex/crpvi47750ad2ea2hfdb/office-schedule-bot:latest`
5. Настройте переменные окружения (см. `DEPLOY.md`)
6. Нажмите **Создать**

Подробная инструкция: см. `DEPLOY.md` или `NEXT_STEPS.md`

