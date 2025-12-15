# Настройка Google Sheets для хранения данных

Этот бот может использовать Google Sheets в качестве хранилища данных вместо локальных файлов. Это позволяет сохранять данные между деплоями на Railway.

## Шаг 1: Создание Google Cloud проекта

1. Перейдите на [Google Cloud Console](https://console.cloud.google.com/)
2. Создайте новый проект или выберите существующий
3. Включите **Google Sheets API** и **Google Drive API**:
   - Перейдите в **APIs & Services** → **Library**
   - Найдите "Google Sheets API" и нажмите **Enable**
   - Найдите "Google Drive API" и нажмите **Enable**

## Шаг 2: Создание Service Account

1. Перейдите в **APIs & Services** → **Credentials**
2. Нажмите **+ CREATE CREDENTIALS** → **Service Account**
3. Заполните:
   - **Service account name**: `office-schedule-bot` (или любое имя)
   - **Service account ID**: будет создан автоматически
4. Нажмите **CREATE AND CONTINUE**
5. Пропустите шаг "Grant this service account access to project" (нажмите **CONTINUE**)
6. Нажмите **DONE**

## Шаг 3: Создание ключа

1. В списке Service Accounts найдите созданный аккаунт и нажмите на него
2. Перейдите на вкладку **KEYS**
3. Нажмите **ADD KEY** → **Create new key**
4. Выберите **JSON** и нажмите **CREATE**
5. Файл JSON будет скачан - сохраните его в безопасном месте

## Шаг 4: Создание Google Sheets таблицы

1. Создайте новую таблицу в [Google Sheets](https://sheets.google.com/)
2. Назовите её, например, "Office Schedule Bot Data"
3. Скопируйте **ID таблицы** из URL:
   - URL выглядит как: `https://docs.google.com/spreadsheets/d/SPREADSHEET_ID/edit`
   - `SPREADSHEET_ID` - это то, что нужно скопировать

## Шаг 5: Предоставление доступа Service Account

1. Откройте созданную таблицу Google Sheets
2. Нажмите кнопку **Share** (Поделиться)
3. В поле "Add people and groups" вставьте **email** из Service Account
   - Email находится в скачанном JSON файле в поле `client_email`
   - Выглядит как: `office-schedule-bot@project-id.iam.gserviceaccount.com`
4. Дайте права **Editor** (Редактор)
5. Нажмите **Send**

## Шаг 6: Настройка переменных окружения в Railway

1. В панели Railway откройте ваш проект
2. Перейдите в раздел **Variables**
3. Добавьте следующие переменные:

### `USE_GOOGLE_SHEETS`
```
true
```

### `GOOGLE_SHEETS_ID`
```
SPREADSHEET_ID
```
(Замените `SPREADSHEET_ID` на ID вашей таблицы)

### `GOOGLE_SHEETS_CREDENTIALS`
```
{"type":"service_account","project_id":"...","private_key_id":"...","private_key":"...","client_email":"...","client_id":"...","auth_uri":"...","token_uri":"...","auth_provider_x509_cert_url":"...","client_x509_cert_url":"..."}
```
(Вставьте весь JSON из скачанного файла как одну строку)

**Важно:** JSON должен быть в одну строку, без переносов строк. Можно использовать онлайн-инструмент для минификации JSON.

## Структура таблицы

Бот автоматически создаст следующие листы в таблице:
- `employees` - список сотрудников
- `admins` - список администраторов
- `default_schedule` - расписание по умолчанию
- `pending_employees` - отложенные записи сотрудников
- `schedules` - расписания на конкретные даты
- `requests` - заявки сотрудников
- `queue` - очередь на дни

## Проверка работы

После настройки перезапустите бота. В логах должно появиться:
```
✅ Google Sheets подключен
```

Если видите:
```
⚠️ Google Sheets не настроен. Используются локальные файлы.
```
Проверьте переменные окружения в Railway.

## Безопасность

- **НЕ** коммитьте JSON файл с credentials в Git
- **НЕ** делитесь JSON файлом публично
- Храните credentials только в переменных окружения Railway
- Используйте отдельный Service Account для каждого проекта

## Отключение Google Sheets

Если хотите вернуться к использованию локальных файлов:
1. Удалите или установите `USE_GOOGLE_SHEETS=false` в Railway Variables
2. Перезапустите бота

Бот автоматически переключится на использование локальных файлов.

