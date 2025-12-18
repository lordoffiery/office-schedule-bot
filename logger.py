"""
Модуль для логирования команд бота
Логи пишутся в PostgreSQL (приоритет) и Google Sheets (для совместимости)
Несохраненные логи буферизуются и отправляются позже
"""
import logging
import asyncio
from datetime import datetime
from collections import deque
from config import TIMEZONE, USE_GOOGLE_SHEETS, SHEET_LOGS, USE_POSTGRESQL
import pytz

timezone = pytz.timezone(TIMEZONE)

# Импортируем Google Sheets Manager только если нужно
sheets_manager = None
if USE_GOOGLE_SHEETS:
    try:
        from google_sheets_manager import GoogleSheetsManager
        sheets_manager = GoogleSheetsManager()
    except ImportError:
        pass
    except Exception as e:
        # Используем стандартный logger для ошибок инициализации
        logging.warning(f"Не удалось инициализировать Google Sheets для логов: {e}")

# Импортируем функции для работы с PostgreSQL
if USE_POSTGRESQL:
    try:
        from database import save_log_to_db, _pool
    except ImportError:
        save_log_to_db = None
        _pool = None
else:
    save_log_to_db = None
    _pool = None

# Создаем logger (без файлового handler, чтобы не занимать место)
logger = logging.getLogger('bot_logger')
logger.setLevel(logging.INFO)

# Буфер для несохраненных логов (из-за ошибок API)
# Формат: deque([(timestamp, user_id, username, first_name, command, response), ...])
_log_buffer = deque(maxlen=1000)  # Максимум 1000 записей в буфере
_last_retry_time = 0  # Время последней попытки отправки буфера
_RETRY_INTERVAL = 60  # Интервал повторной попытки в секундах


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
    
    # Формируем строку для таблицы: [timestamp, user_id, username, first_name, command, response]
    username_str = username if username else ""
    row = [
        timestamp,
        str(user_id),
        username_str,
        first_name,
        command,
        response_short
    ]
    
    # Сохраняем в PostgreSQL (приоритет 1)
    if USE_POSTGRESQL and _pool and save_log_to_db:
        try:
            try:
                loop = asyncio.get_running_loop()
                asyncio.run_coroutine_threadsafe(
                    save_log_to_db(user_id, username or '', first_name, command, response_short),
                    loop
                )
            except RuntimeError:
                asyncio.run(save_log_to_db(user_id, username or '', first_name, command, response_short))
        except Exception as e:
            logger.warning(f"Ошибка записи лога в PostgreSQL: {e}")
            # Добавляем в буфер для повторной попытки
            _log_buffer.append(('postgresql', row))
    
    # Сохраняем в Google Sheets (приоритет 2, для совместимости)
    if sheets_manager and sheets_manager.is_available():
        try:
            # Используем PRIORITY_LOW для логов - они будут пропущены при превышении лимита API
            from google_sheets_manager import PRIORITY_LOW
            success = sheets_manager.append_row(SHEET_LOGS, row, priority=PRIORITY_LOW)
            
            # Если не удалось записать (например, из-за лимита API), добавляем в буфер
            if not success:
                _log_buffer.append(('sheets', row))
                logger.debug(f"Лог добавлен в буфер Google Sheets (размер буфера: {len(_log_buffer)})")
        except Exception as e:
            # Обрабатываем только критические ошибки (не 429)
            # Ошибка 429 (превышение лимита) обрабатывается внутри append_row и не попадает сюда
            # Логируем только другие ошибки
            error_str = str(e)
            if '429' not in error_str and 'Quota exceeded' not in error_str:
                logging.warning(f"Ошибка записи лога в Google Sheets: {e}")
            # При любой ошибке добавляем в буфер
            _log_buffer.append(('sheets', row))
    else:
        # Если Google Sheets недоступен, используем стандартный logger (но не файл)
        logger.info(log_message)
        # Также добавляем в буфер на случай, если Google Sheets станет доступен позже
        _log_buffer.append(('sheets', row))


async def flush_log_buffer():
    """
    Периодическая задача для отправки буферизованных логов
    Вызывается каждые 60 секунд
    """
    global _last_retry_time
    
    while True:
        try:
            await asyncio.sleep(_RETRY_INTERVAL)
            
            # Проверяем, есть ли что отправлять
            if not _log_buffer:
                continue
            
            if not sheets_manager or not sheets_manager.is_available():
                continue
            
            # Пытаемся отправить все логи из буфера
            sent_count = 0
            failed_count = 0
            failed_logs = []
            
            # Отправляем логи по одному, чтобы не перегрузить API
            while _log_buffer:
                log_entry = _log_buffer[0]
                target, row = log_entry
                
                success = False
                if target == 'postgresql' and USE_POSTGRESQL and _pool and save_log_to_db:
                    try:
                        # Парсим row для PostgreSQL
                        user_id = int(row[1]) if len(row) > 1 else 0
                        username = row[2] if len(row) > 2 else ''
                        first_name = row[3] if len(row) > 3 else ''
                        command = row[4] if len(row) > 4 else ''
                        response = row[5] if len(row) > 5 else ''
                        
                        await save_log_to_db(user_id, username, first_name, command, response)
                        success = True
                    except Exception as e:
                        logger.warning(f"Ошибка отправки лога в PostgreSQL из буфера: {e}")
                
                elif target == 'sheets' and sheets_manager and sheets_manager.is_available():
                    try:
                        from google_sheets_manager import PRIORITY_LOW
                        success = sheets_manager.append_row(SHEET_LOGS, row, priority=PRIORITY_LOW)
                    except Exception as e:
                        logger.warning(f"Ошибка отправки лога в Google Sheets из буфера: {e}")
                
                if success:
                    _log_buffer.popleft()
                    sent_count += 1
                else:
                    # Если не удалось отправить, сохраняем для следующей попытки
                    failed_logs.append(_log_buffer.popleft())
                    failed_count = len(_log_buffer) + len(failed_logs)
                    # Если слишком много неудачных попыток, останавливаемся
                    if len(failed_logs) > 100:
                        break
            
            # Возвращаем неудачные логи обратно в буфер
            for log_entry in failed_logs:
                _log_buffer.appendleft(log_entry)
            
            if sent_count > 0:
                logger.info(f"Отправлено {sent_count} логов из буфера (осталось: {failed_count})")
            
            _last_retry_time = datetime.now(timezone).timestamp()
        except Exception as e:
            logger.error(f"Ошибка при отправке буферизованных логов: {e}")


def start_log_buffer_flusher():
    """Запустить задачу для периодической отправки буферизованных логов"""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # Если цикл уже запущен, создаем задачу
            asyncio.create_task(flush_log_buffer())
        else:
            # Если цикл не запущен, запускаем в новом потоке
            loop.run_until_complete(flush_log_buffer())
    except RuntimeError:
        # Если нет event loop, создаем новый
        asyncio.create_task(flush_log_buffer())

