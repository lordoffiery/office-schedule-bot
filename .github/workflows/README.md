# GitHub Actions для автоматического деплоя

## Настройка секретов в GitHub

Перед использованием workflow необходимо добавить секреты в репозиторий:

1. Откройте ваш репозиторий на GitHub
2. Перейдите в **Settings** → **Secrets and variables** → **Actions**
3. Нажмите **New repository secret**
4. Добавьте следующие секреты:

### Необходимые секреты:

#### 1. `YC_REGISTRY_ID`
- **Описание:** ID вашего Container Registry в Yandex Cloud
- **Где найти:** Yandex Cloud Console → Container Registry → ваш registry → ID
- **Пример:** `crp1234567890abcdef`

#### 2. `YC_SA_JSON_CREDENTIALS`
- **Описание:** JSON ключ Service Account для доступа к Container Registry
- **Как получить:**
  1. Yandex Cloud Console → IAM → Service Accounts
  2. Создайте Service Account (или используйте существующий)
  3. Назначьте роль: `container-registry.images.pusher`
  4. Создайте ключ: Service Account → Keys → Create key → JSON
  5. Скопируйте весь JSON (минифицированный, в одну строку)
  6. Вставьте в секрет `YC_SA_JSON_CREDENTIALS`

### Пример JSON ключа:
```json
{"id":"ajek123...","service_account_id":"ajet123...","created_at":"2024-01-01T00:00:00.000000Z","key_algorithm":"RSA_2048","public_key":"-----BEGIN PUBLIC KEY-----\n...\n-----END PUBLIC KEY-----\n","private_key":"-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"}
```

## Как это работает

1. При каждом push в ветку `main` автоматически:
   - Собирается Docker образ
   - Загружается в Yandex Container Registry
   - Тегируется как `latest` и с SHA коммита

2. Можно запустить вручную:
   - GitHub → Actions → Build and Push to Yandex Cloud → Run workflow

## Использование образа

После успешного выполнения workflow используйте образ в Serverless Container:

```
cr.yandex/<YC_REGISTRY_ID>/office-schedule-bot:latest
```

## Troubleshooting

### Ошибка авторизации
- Проверьте, что `YC_SA_JSON_CREDENTIALS` содержит полный JSON ключ
- Убедитесь, что Service Account имеет роль `container-registry.images.pusher`

### Ошибка при сборке
- Проверьте логи в GitHub Actions
- Убедитесь, что все файлы на месте (Dockerfile, requirements.txt)

### Образ не загружается
- Проверьте, что `YC_REGISTRY_ID` указан правильно
- Убедитесь, что Service Account имеет доступ к registry

