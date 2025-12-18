"""
Конфигурация бота
"""
import os
from typing import List

# Telegram Bot Token (получите у @BotFather)
# Устанавливается через переменную окружения BOT_TOKEN
API_TOKEN = os.getenv('BOT_TOKEN')
if not API_TOKEN:
    raise ValueError("BOT_TOKEN environment variable is required")

# ID администраторов (список Telegram user IDs)
ADMIN_IDS: List[int] = [int(x) for x in os.getenv('ADMIN_IDS', '312551109').split(',') if x]

# Часовой пояс
TIMEZONE = 'Europe/Moscow'

# Время напоминания (пятница, конец рабочего дня)
REMINDER_HOUR = 18  # 18:00
REMINDER_MINUTE = 0

# Время рассылки расписания (воскресенье вечером)
SCHEDULE_SEND_HOUR = 20  # 20:00
SCHEDULE_SEND_MINUTE = 0

# Пути к файлам данных
DATA_DIR = 'data'
SCHEDULES_DIR = f'{DATA_DIR}/schedules'
REQUESTS_DIR = f'{DATA_DIR}/requests'
QUEUE_DIR = f'{DATA_DIR}/queue'
EMPLOYEES_FILE = f'{DATA_DIR}/employees.txt'
ADMINS_FILE = f'{DATA_DIR}/admins.txt'
DEFAULT_SCHEDULE_FILE = f'{DATA_DIR}/default_schedule.txt'
PENDING_EMPLOYEES_FILE = f'{DATA_DIR}/pending_employees.txt'

# PostgreSQL настройки
DATABASE_URL = os.getenv('DATABASE_URL') or os.getenv('DATABASE_PUBLIC_URL')
USE_POSTGRESQL = DATABASE_URL is not None  # Автоматически определяется наличием DATABASE_URL

# Google Sheets настройки (опционально, используется одновременно с PostgreSQL для синхронизации)
# Во время миграции оба хранилища работают одновременно
USE_GOOGLE_SHEETS = os.getenv('USE_GOOGLE_SHEETS', 'true').lower() == 'true'
GOOGLE_SHEETS_ID = os.getenv('GOOGLE_SHEETS_ID', '13zmdoS160B5Hn0Cl-q2hNrEgyZVc6Jh0JaxUnI9jSFg')  # ID из URL
GOOGLE_SHEETS_CREDENTIALS = os.getenv('GOOGLE_SHEETS_CREDENTIALS', '')
GOOGLE_CREDENTIALS_FILE = os.getenv('GOOGLE_CREDENTIALS_FILE', 'google_credentials.json')

# Названия листов в Google Sheets
SHEET_EMPLOYEES = 'employees'
SHEET_ADMINS = 'admins'
SHEET_DEFAULT_SCHEDULE = 'default_schedule'
SHEET_PENDING_EMPLOYEES = 'pending_employees'
SHEET_SCHEDULES = 'schedules'
SHEET_REQUESTS = 'requests'
SHEET_QUEUE = 'queue'
SHEET_LOGS = 'logs'

# Максимальное количество мест в офисе
MAX_OFFICE_SEATS = 8

# Расписание по умолчанию (JSON формат: день -> {подразделение.место: имя})
# Формат ключа: "подразделение.место" (например, "1.1", "1.2", "2.1")
# Обновлено согласно алгоритму приоритетов (количество дней в неделю)
# Имена без username - username добавляется автоматически при форматировании
DEFAULT_SCHEDULE = {
    'Понедельник': {
        '1.1': 'Дима Ч', '1.2': 'Тимур', '1.3': 'Вася', '1.4': 'Илья',
        '1.5': 'Егор', '1.6': 'Айлар', '1.7': 'Виталий', '1.8': 'Даша'
    },
    'Вторник': {
        '1.1': 'Дима Ч', '1.2': 'Тимур', '1.3': 'Вася', '1.4': 'Айдан',
        '1.5': 'Рома', '1.6': 'Дима А', '1.7': 'Костя', '1.8': 'Леша Б'
    },
    'Среда': {
        '1.1': 'Дима Ч', '1.2': 'Тимур', '1.3': 'Костя', '1.4': 'Илья',
        '1.5': 'Рома', '1.6': 'Катя', '1.7': 'Артем', '1.8': 'Марк'
    },
    'Четверг': {
        '1.1': 'Дима Ч', '1.2': 'Тимур', '1.3': 'Вася', '1.4': 'Леша Б',
        '1.5': 'Рома', '1.6': 'Марк', '1.7': 'Толя', '1.8': 'Глеб'
    },
    'Пятница': {
        '1.1': 'Дима Ч', '1.2': 'Тимур', '1.3': 'Вася', '1.4': 'Илья',
        '1.5': 'Егор', '1.6': 'Айлар', '1.7': 'Виталий', '1.8': 'Даша'
    }
}

# Дни недели для парсинга
WEEKDAYS_RU = {
    'понедельник': 'Понедельник',
    'вторник': 'Вторник',
    'среда': 'Среда',
    'четверг': 'Четверг',
    'пятница': 'Пятница',
    'пн': 'Понедельник',
    'вт': 'Вторник',
    'ср': 'Среда',
    'чт': 'Четверг',
    'пт': 'Пятница'
}

