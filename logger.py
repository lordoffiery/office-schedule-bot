"""
Модуль для логирования команд бота
"""
import os
import logging
from datetime import datetime
from config import DATA_DIR, TIMEZONE
import pytz

# Создаем директорию для логов
LOGS_DIR = f'{DATA_DIR}/logs'
os.makedirs(LOGS_DIR, exist_ok=True)

# Настройка логирования
log_file = os.path.join(LOGS_DIR, 'bot.log')
timezone = pytz.timezone(TIMEZONE)

# Настраиваем форматтер
formatter = logging.Formatter(
    '%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Создаем handler для файла
file_handler = logging.FileHandler(log_file, encoding='utf-8')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

# Создаем logger
logger = logging.getLogger('bot_logger')
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)


def log_command(user_id: int, username: str, first_name: str, command: str, response: str):
    """
    Логировать команду пользователя и ответ бота
    
    Args:
        user_id: Telegram ID пользователя
        username: Username пользователя (может быть None)
        first_name: Имя пользователя
        command: Команда, которую выполнил пользователь
        response: Ответ бота (первые 200 символов)
    """
    now = datetime.now(timezone)
    timestamp = now.strftime('%Y-%m-%d %H:%M:%S')
    
    # Формируем информацию о пользователе
    user_info = f"ID:{user_id}"
    if username:
        user_info += f" @{username}"
    if first_name:
        user_info += f" ({first_name})"
    
    # Обрезаем ответ, если он слишком длинный
    response_short = response[:200] + "..." if len(response) > 200 else response
    # Заменяем переносы строк на пробелы для читаемости
    response_short = response_short.replace('\n', ' | ')
    
    log_message = f"{user_info} | Команда: {command} | Ответ: {response_short}"
    logger.info(log_message)

