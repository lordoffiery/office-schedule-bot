"""
Вспомогательные утилиты для работы с Google Sheets и общие функции
"""
import logging
from typing import List, Tuple, Optional, Callable, Any
from functools import wraps

# Опциональный импорт aiogram (нужен только для декораторов)
try:
    from aiogram.types import Message
except ImportError:
    Message = None  # Для тестирования без aiogram

logger = logging.getLogger(__name__)


def get_header_start_idx(rows: List[List[str]], header_keywords: List[str]) -> Tuple[int, bool]:
    """
    Определить индекс начала данных (пропуская заголовок) и наличие заголовка
    
    Args:
        rows: Список строк из Google Sheets
        header_keywords: Список ключевых слов для определения заголовка
        
    Returns:
        Tuple[int, bool]: (start_idx, has_header)
    """
    if not rows or len(rows) == 0:
        return 0, False
    
    first_row = rows[0]
    if not first_row or len(first_row) == 0:
        return 0, False
    
    first_cell = first_row[0].strip() if first_row[0] else ''
    has_header = first_cell in header_keywords
    
    return 1 if has_header else 0, has_header


def filter_empty_rows(rows: List[List[str]]) -> List[List[str]]:
    """
    Отфильтровать пустые строки из списка строк
    
    Args:
        rows: Список строк
        
    Returns:
        Отфильтрованный список строк
    """
    return [row for row in rows if row and any(cell.strip() for cell in row if cell)]


def ensure_header(rows: List[List[str]], default_header: List[str], 
                  header_keywords: List[str]) -> List[List[str]]:
    """
    Убедиться, что в списке строк есть заголовок
    
    Args:
        rows: Список строк
        default_header: Заголовок по умолчанию
        header_keywords: Ключевые слова для определения заголовка
        
    Returns:
        Список строк с гарантированным заголовком
    """
    rows = filter_empty_rows(rows)
    
    if not rows:
        return [default_header]
    
    first_row = rows[0]
    if not first_row or len(first_row) == 0:
        return [default_header] + rows
    
    first_cell = first_row[0].strip() if first_row[0] else ''
    has_header = first_cell in header_keywords
    
    if has_header:
        return rows
    else:
        return [default_header] + rows


def check_user_registered_and_approved(employee_manager) -> Callable:
    """
    Декоратор для проверки регистрации и одобрения пользователя админом
    
    Args:
        employee_manager: Менеджер сотрудников
        
    Returns:
        Декоратор функции
    """
    if Message is None:
        raise ImportError("aiogram не установлен, декоратор недоступен")
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(message: Message, *args, **kwargs):
            from main import get_user_info, log_command
            
            user_id = message.from_user.id
            user_info = get_user_info(message)
            
            if not employee_manager.is_registered(user_id):
                response = "Вы не зарегистрированы. Используйте /start"
                await message.reply(response)
                log_command(user_info['user_id'], user_info['username'], user_info['first_name'], 
                           func.__name__.replace('cmd_', '/'), response)
                return
            
            if not employee_manager.was_added_by_admin(user_id):
                response = (
                    "❌ Для использования этой команды необходимо, чтобы администратор добавил вас в систему.\n\n"
                    "Обратитесь к администратору для получения доступа."
                )
                await message.reply(response)
                log_command(user_info['user_id'], user_info['username'], user_info['first_name'],
                           func.__name__.replace('cmd_', '/'), response)
                return
            
            employee_name = employee_manager.get_employee_name(user_id)
            if not employee_name:
                response = "Ошибка: не найдено ваше имя в системе"
                await message.reply(response)
                log_command(user_info['user_id'], user_info['username'], user_info['first_name'],
                           func.__name__.replace('cmd_', '/'), response)
                return
            
            # Передаем employee_name в функцию
            return await func(message, employee_name, user_info, *args, **kwargs)
        return wrapper
    return decorator


def check_admin(func: Callable) -> Callable:
    """
    Декоратор для проверки прав администратора
    
    Args:
        func: Функция-обработчик команды
        
    Returns:
        Обернутая функция
    """
    if Message is None:
        raise ImportError("aiogram не установлен, декоратор недоступен")
    
    @wraps(func)
    async def wrapper(message: Message, *args, **kwargs):
        from main import admin_manager, get_user_info, log_command
        
        user_id = message.from_user.id
        user_info = get_user_info(message)
        
        if not admin_manager.is_admin(user_id):
            response = "Эта команда доступна только администраторам"
            await message.reply(response)
            log_command(user_info['user_id'], user_info['username'], user_info['first_name'],
                       func.__name__.replace('cmd_', '/'), response)
            return
        
        return await func(message, user_info, *args, **kwargs)
    return wrapper

