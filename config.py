"""
Конфигурация бота
"""
import os
from typing import List

# Telegram Bot Token (получите у @BotFather)
API_TOKEN = os.getenv('BOT_TOKEN', '7770460363:AAEGPCzfjdhvmOomQf1FyBoCV3Xyl2etibU')

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

# Максимальное количество мест в офисе
MAX_OFFICE_SEATS = 8

# Расписание по умолчанию
DEFAULT_SCHEDULE = {
    'Понедельник': ['Вася', 'Дима Ч', 'Айлар', 'Егор', 'Илья', 'Даша', 'Виталий', 'Тимур'],
    'Вторник': ['Вася', 'Дима Ч', 'Айдан', 'Дима А', 'Тимур', 'Рома', 'Костя', 'Леша Б'],
    'Среда': ['Дима Ч', 'Илья', 'Тимур', 'Костя', 'Катя', 'Артем', 'Рома', 'Марк'],
    'Четверг': ['Вася', 'Дима Ч', 'Толя', 'Глеб', 'Тимур', 'Рома', 'Леша Б', 'Марк'],
    'Пятница': ['Вася', 'Дима Ч', 'Айлар', 'Егор', 'Илья', 'Даша', 'Виталий', 'Тимур']
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

