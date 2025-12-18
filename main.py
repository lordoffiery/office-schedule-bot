"""
Основной файл Telegram-бота для управления расписанием сотрудников
"""
import asyncio
import os
import logging
import threading
from datetime import datetime, timedelta
from typing import Optional
from http.server import HTTPServer, BaseHTTPRequestHandler
from aiogram import Bot, Dispatcher, BaseMiddleware
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from typing import Callable, Dict, Any, Awaitable

from config import API_TOKEN, ADMIN_IDS, WEEKDAYS_RU, TIMEZONE, MAX_OFFICE_SEATS, SCHEDULES_DIR, SHEET_SCHEDULES
from employee_manager import EmployeeManager
from schedule_manager import ScheduleManager
from notification_manager import NotificationManager
from admin_manager import AdminManager
from logger import log_command
from init_data import init_all
import pytz

# Настройка логирования
logger = logging.getLogger(__name__)


# Инициализация
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Менеджеры
admin_manager = AdminManager()
employee_manager = EmployeeManager()
schedule_manager = ScheduleManager(employee_manager)
notification_manager = NotificationManager(bot, schedule_manager, employee_manager, admin_manager)

timezone = pytz.timezone(TIMEZONE)


# Простой HTTP-сервер для health check
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health' or self.path == '/':
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'OK')
        else:
            self.send_response(404)
            self.end_headers()
    
    def log_message(self, format, *args):
        # Отключаем логирование HTTP-запросов
        pass

def start_health_server():
    """Запустить простой HTTP-сервер для health check на порту 8080"""
    try:
        server = HTTPServer(('0.0.0.0', 8080), HealthCheckHandler)
        logger.info("Health check server started on port 8080")
        server.serve_forever()
    except Exception as e:
        logger.warning(f"Не удалось запустить health check server: {e}")


# Вспомогательная функция для логирования команд
def get_user_info(message: Message):
    """Получить информацию о пользователе для логирования"""
    return {
        'user_id': message.from_user.id,
        'username': message.from_user.username,
        'first_name': message.from_user.first_name or "Пользователь"
    }


# Вспомогательные функции
def parse_weekdays(text: str) -> list:
    """Парсинг дней недели из текста"""
    text = text.lower().strip()
    days = []
    
    # Разделяем по запятым, пробелам
    parts = text.replace(',', ' ').split()
    
    for part in parts:
        part = part.strip()
        if part in WEEKDAYS_RU:
            day_name = WEEKDAYS_RU[part]
            if day_name not in days:
                days.append(day_name)
    
    return days


def day_to_short(day: str) -> str:
    """Преобразовать полное название дня в сокращенное"""
    day_map = {
        'Понедельник': 'Пн',
        'Вторник': 'Вт',
        'Среда': 'Ср',
        'Четверг': 'Чт',
        'Пятница': 'Пт'
    }
    return day_map.get(day, day[:2])


def get_main_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """Создать основную клавиатуру с кнопками команд"""
    is_admin = admin_manager.is_admin(user_id)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📅 Моё расписание", callback_data="cmd_my_schedule"),
            InlineKeyboardButton(text="📋 Полное расписание", callback_data="cmd_full_schedule")
        ],
        [
            InlineKeyboardButton(text="➕ Добавить день", callback_data="cmd_add_day"),
            InlineKeyboardButton(text="➖ Пропустить день", callback_data="cmd_skip_day")
        ],
        [
            InlineKeyboardButton(text="📝 Указать дни недели", callback_data="cmd_set_week_days"),
            InlineKeyboardButton(text="ℹ️ Помощь", callback_data="cmd_help")
        ]
    ])
    
    if is_admin:
        # Добавляем админские кнопки
        admin_buttons = [
            [
                InlineKeyboardButton(text="👤 Добавить сотрудника", callback_data="cmd_admin_add_employee"),
                InlineKeyboardButton(text="👑 Добавить админа", callback_data="cmd_admin_add_admin")
            ],
            [
                InlineKeyboardButton(text="📊 Список админов", callback_data="cmd_admin_list_admins"),
                InlineKeyboardButton(text="🔄 Синхронизация", callback_data="cmd_admin_sync_from_sheets")
            ]
        ]
        keyboard.inline_keyboard.extend(admin_buttons)
    
    return keyboard


# Глобальные переменные для синхронизации
_sync_lock = None
_last_sync_time = 0

# Функция синхронизации PostgreSQL -> Google Sheets
async def sync_postgresql_to_sheets():
    """Синхронизация данных из PostgreSQL в Google Sheets (неблокирующая)"""
    global _last_sync_time, _sync_lock
    
    if _sync_lock is None:
        return  # Бот еще не инициализирован
    
    from config import USE_GOOGLE_SHEETS
    if not USE_GOOGLE_SHEETS:
        return
    
    # Проверяем, не синхронизировали ли мы недавно (защита от частых вызовов)
    current_time = asyncio.get_event_loop().time()
    if current_time - _last_sync_time < 5:  # Минимум 5 секунд между синхронизациями
        return
    
    async with _sync_lock:
        # Двойная проверка после получения блокировки
        current_time = asyncio.get_event_loop().time()
        if current_time - _last_sync_time < 5:
            return
        
        try:
            # Запускаем синхронизацию в фоне (не блокируем выполнение команд)
            asyncio.create_task(_run_sync_in_background())
            _last_sync_time = current_time
        except Exception as e:
            logger.error(f"❌ Ошибка при запуске синхронизации: {e}", exc_info=True)

async def _run_sync_in_background():
    """Запустить синхронизацию в фоновом режиме"""
    try:
        import subprocess
        import sys
        # Запускаем скрипт синхронизации в отдельном процессе (не блокируем)
        subprocess.Popen(
            [sys.executable, 'sync_postgresql_to_sheets.py'],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        logger.debug("🔄 Запущена фоновая синхронизация PostgreSQL -> Google Sheets")
    except Exception as e:
        logger.error(f"❌ Ошибка при запуске фоновой синхронизации: {e}", exc_info=True)

# Middleware для автоматической синхронизации после каждой команды
# SyncMiddleware удален - синхронизация теперь происходит только после команд, изменяющих данные


def format_schedule_with_places(schedule: dict, default_schedule: dict = None) -> str:
    """
    Форматировать расписание с указанием мест для каждого сотрудника
    
    Args:
        schedule: Dict[str, List[str]] - расписание в формате {день: [имена]}
        default_schedule: Dict[str, Dict[str, str]] - расписание по умолчанию в формате {день: {место: имя}}
        
    Returns:
        str - отформатированное расписание с местами
    """
    # Загружаем default_schedule, если не передан
    if default_schedule is None:
        default_schedule = schedule_manager.load_default_schedule()
    
    def parse_place_key(place_key: str) -> tuple:
        """Парсит ключ места (например, '1.6') в кортеж для сортировки (1, 6)"""
        try:
            parts = place_key.split('.')
            if len(parts) == 2:
                return (int(parts[0]), int(parts[1]))
            return (999, 999)  # Для некорректных форматов - в конец
        except (ValueError, IndexError):
            return (999, 999)
    
    result = []
    for day, employees in schedule.items():
        if employees:
            # Форматируем список сотрудников с местами
            employees_with_places = []
            for emp in employees:
                # Получаем простое имя из отформатированного (если есть username)
                plain_name = schedule_manager.get_plain_name_from_formatted(emp)
                
                # Ищем место сотрудника в default_schedule для этого дня
                place = None
                if day in default_schedule:
                    places_dict = default_schedule[day]
                    for place_key, name in places_dict.items():
                        # Сравниваем простые имена
                        plain_name_in_schedule = schedule_manager.get_plain_name_from_formatted(name)
                        if plain_name_in_schedule == plain_name:
                            place = place_key
                            break
                
                # Если место не найдено, используем порядковый номер в списке
                if place is None:
                    # Находим индекс сотрудника в списке
                    try:
                        index = employees.index(emp) + 1
                        place = f"1.{index}"
                    except ValueError:
                        place = "1.999"  # Для сортировки
                
                employees_with_places.append((place, emp))
            
            # Сортируем по номеру места (1.1, 1.2, 1.3...)
            employees_with_places.sort(key=lambda x: parse_place_key(x[0]))
            
            # Форматируем отсортированный список - каждый сотрудник на отдельной строке
            day_lines = [f"{day}:"]
            for place, emp in employees_with_places:
                day_lines.append(f"  {place}: {emp}")
            result.append("\n".join(day_lines))
        else:
            result.append(f"{day}: (пусто)")
    return "\n\n".join(result)


def format_schedule_message(employee_schedule: dict, week_start: datetime) -> str:
    """Форматировать сообщение с расписанием"""
    week_dates = schedule_manager.get_week_dates(week_start)
    week_str = f"{week_dates[0][0].strftime('%d.%m')} - {week_dates[-1][0].strftime('%d.%m.%Y')}"
    
    office_days = [day for day, in_office in employee_schedule.items() if in_office]
    remote_days = [day for day, in_office in employee_schedule.items() if not in_office]
    
    message = f"📅 Ваше расписание на неделю {week_str}:\n\n"
    
    if office_days:
        office_days_short = [day_to_short(day) for day in office_days]
        message += f"🏢 Дни в офисе: {', '.join(office_days_short)}\n"
    
    if remote_days:
        remote_days_short = [day_to_short(day) for day in remote_days]
        message += f"🏠 Дни удаленно: {', '.join(remote_days_short)}\n"
    
    return message


# Команды бота
@dp.message(Command("start"))
async def cmd_start(message: Message):
    """Команда /start"""
    user_id = message.from_user.id
    user_name = message.from_user.first_name or "Пользователь"
    username = message.from_user.username
    
    # Регистрируем пользователя, если его еще нет
    was_registered = employee_manager.is_registered(user_id)
    was_new, was_added_by_admin = employee_manager.register_user(user_id, user_name, username)
    
    # Если пользователь был добавлен админом (через pending или напрямую), обновляем default_schedule и schedules
    if was_added_by_admin:
        employee_name = employee_manager.get_employee_name(user_id)
        if employee_name:
            # Форматируем имя с username
            formatted_name = employee_manager.format_employee_name_by_id(user_id)
            # Обновляем имя в default_schedule (добавляем username в скобках)
            schedule_manager.update_employee_name_in_default_schedule(employee_name, formatted_name)
            # Обновляем имя во всех расписаниях в Google Sheets (вкладка schedules)
            schedule_manager.update_employee_name_in_schedules(employee_name, formatted_name)
    
    if was_new and not was_added_by_admin:
        # Пользователь сам себя зарегистрировал, не был добавлен админом
        response = (
            f"Привет, {user_name}!\n\n"
            "Вы зарегистрированы в системе, но для полного доступа к функциям бота "
            "необходимо, чтобы администратор добавил вас через команду /admin_add_employee.\n\n"
            "Обратитесь к администратору для получения доступа.\n\n"
            "Используйте /help для списка доступных команд."
        )
    elif was_new and was_added_by_admin:
        # Пользователь был добавлен админом и только что зарегистрировался
        response = (
            f"Привет, {user_name}! Я бот для управления расписанием сотрудников.\n\n"
            "Используйте /help для списка команд."
        )
    else:
        # Пользователь уже был зарегистрирован
        response = "Вы уже зарегистрированы! Используйте /help для списка команд."
    
    keyboard = get_main_keyboard(user_id)
    await message.reply(response, reply_markup=keyboard)
    log_command(user_id, username, user_name, "/start", response)
    # Синхронизируем только если пользователь был зарегистрирован (данные изменились)
    if was_new:
        await sync_postgresql_to_sheets()


@dp.message(Command("help"))
async def cmd_help(message: Message):
    """Команда /help"""
    user_id = message.from_user.id
    username = message.from_user.username
    first_name = message.from_user.first_name or "Пользователь"
    is_admin = admin_manager.is_admin(user_id)
    
    help_text = (
        "📋 Доступные команды:\n\n"
        "📅 Управление расписанием:\n"
        "/set_week_days [даты] - Указать дни на следующую неделю\n"
        "   Пример: /set_week_days 2024-12-23 2024-12-24 2024-12-26\n"
        "   Также можно: /set_week_days пн вт чт\n\n"
        "/my_schedule - Показать свое расписание на текущую неделю\n\n"
        "/skip_day [дата] - Пропустить день (можно указать несколько дат)\n"
        "   Пример: /skip_day 2024-12-20\n"
        "   Пример: /skip_day 2024-12-20 2024-12-21\n\n"
        "/add_day [дата] - Запросить дополнительный день (можно указать несколько дат)\n"
        "   Пример: /add_day 2024-12-20\n"
        "   Пример: /add_day 2024-12-20 2024-12-21\n\n"
    )
    
    if is_admin:
        help_text += (
            "\n👑 Админские команды:\n"
            "/full_schedule [дата] - Полное расписание на дату\n\n"
            "/admin_add_employee [имя] @username - Добавить сотрудника\n\n"
            "/admin_add_admin @username - Добавить администратора\n\n"
            "/admin_list_admins - Список администраторов\n\n"
            "/admin_test_schedule - Тестовая рассылка расписания\n\n"
            "/admin_skip_day @username [дата] - Пропустить день для сотрудника\n"
            "   Пример: /admin_skip_day @username 2024-12-20\n"
            "   Пример: /admin_skip_day @username 2024-12-20 2024-12-21\n\n"
            "/admin_add_day @username [дата] - Добавить день для сотрудника\n"
            "   Пример: /admin_add_day @username 2024-12-20\n"
            "   Пример: /admin_add_day @username 2024-12-20 2024-12-21\n\n"
            "/admin_set_default_schedule [день] [список сотрудников] - Установить расписание по умолчанию для дня\n"
            "   Пример: /admin_set_default_schedule Понедельник Вася, Дима Ч, Айлар, Егор, Илья, Даша, Виталий, Тимур\n"
            "   Дни: Понедельник, Вторник, Среда, Четверг, Пятница\n\n"
            "/admin_refresh_schedules - Обновить имена сотрудников в расписаниях (синхронизация с employees)\n"
            "   Используйте после ручного добавления сотрудников в Google Sheets\n\n"
            "/admin_sync_from_sheets - Синхронизировать данные из Google Sheets в PostgreSQL\n"
            "   Используйте после ручного изменения данных в Google Sheets"
        )
    
    keyboard = get_main_keyboard(user_id)
    await message.reply(help_text, reply_markup=keyboard)
    log_command(user_id, username, first_name, "/help", help_text[:200])


@dp.message(Command("set_week_days"))
async def cmd_set_week_days(message: Message):
    """Команда для установки дней на следующую неделю (поддерживает даты и названия дней)"""
    user_id = message.from_user.id
    
    user_info = get_user_info(message)
    
    if not employee_manager.is_registered(user_id):
        response = "Вы не зарегистрированы. Используйте /start"
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/set_week_days", response)
        return
    
    # Проверяем, был ли пользователь добавлен админом
    if not employee_manager.was_added_by_admin(user_id):
        response = (
            "❌ Для использования этой команды необходимо, чтобы администратор добавил вас в систему.\n\n"
            "Обратитесь к администратору для получения доступа."
        )
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/set_week_days", response)
        return
    
    employee_name = employee_manager.get_employee_name(user_id)
    if not employee_name:
        response = "Ошибка: не найдено ваше имя в системе"
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/set_week_days", response)
        return
    
    # Парсим аргументы из команды
    command_parts = message.text.split()
    if len(command_parts) < 2:
        response = (
            "Укажите дни недели. Например:\n"
            "/set_week_days 2024-12-23 2024-12-24 2024-12-26\n"
            "или: /set_week_days пн вт чт"
        )
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/set_week_days", response)
        return
    
    # Получаем начало следующей недели
    now = datetime.now(timezone)
    next_week_start = schedule_manager.get_week_start(now + timedelta(days=7))
    week_dates = schedule_manager.get_week_dates(next_week_start)
    
    # Пытаемся распарсить как даты
    days = []
    dates_parsed = False
    
    for arg in command_parts[1:]:
        try:
            # Пытаемся распарсить как дату
            date = datetime.strptime(arg, "%Y-%m-%d")
            date = timezone.localize(date)
            
            # Проверяем, что дата относится к следующей неделе
            if schedule_manager.get_week_start(date) == next_week_start:
                # Определяем день недели для этой даты
                for d, day_n in week_dates:
                    if d.date() == date.date():
                        if day_n not in days:
                            days.append(day_n)
                        dates_parsed = True
                        break
        except ValueError:
            # Не дата, пытаемся распарсить как название дня
            pass
    
    # Если не удалось распарсить как даты, пытаемся как названия дней
    if not dates_parsed:
        days_text = ' '.join(command_parts[1:])
        days = parse_weekdays(days_text)
        
        if not days:
            response = (
                "Не удалось распознать дни. Используйте формат:\n"
                "/set_week_days 2024-12-23 2024-12-24 2024-12-26\n"
                "или: /set_week_days пн вт чт\n"
                "или: /set_week_days понедельник вторник четверг"
            )
            await message.reply(response)
            log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/set_week_days", response)
            return
    
    # Определяем, какие дни нужно пропустить (если есть в расписании по умолчанию)
    default_schedule = schedule_manager.load_default_schedule()
    days_to_skip = []
    days_to_request = []
    guaranteed_days = []  # Дни из расписания по умолчанию, которые указаны в команде
    additional_days = []  # Дни, которых нет в расписании по умолчанию, но указаны в команде
    
    week_days = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница']
    for day in week_days:
        if day in default_schedule:
            # Проверяем, есть ли сотрудник в расписании (новый формат: словарь мест)
            places_dict = default_schedule[day]
            employee_in_schedule = False
            for place_key, emp in places_dict.items():
                plain_name = schedule_manager.get_plain_name_from_formatted(emp)
                if plain_name == employee_name:
                    employee_in_schedule = True
                    break
            
            if employee_in_schedule:
                # Есть в расписании по умолчанию
                if day not in days:
                    days_to_skip.append(day)
                else:
                    days_to_request.append(day)
                    guaranteed_days.append(day)
            else:
                # Нет в расписании по умолчанию
                if day in days:
                    days_to_request.append(day)
                    additional_days.append(day)
        else:
            # Дня нет в расписании по умолчанию
            if day in days:
                days_to_request.append(day)
                additional_days.append(day)
    
    # Загружаем существующие заявки и удаляем старую заявку пользователя
    requests = schedule_manager.load_requests_for_week(next_week_start)
    
    # Очищаем файл заявок и пересохраняем все, кроме заявки текущего пользователя
    schedule_manager.clear_requests_for_week(next_week_start)
    for req in requests:
        if req['employee_name'] != employee_name or req['telegram_id'] != user_id:
            schedule_manager.save_request(
                req['employee_name'], req['telegram_id'], next_week_start,
                req['days_requested'], req['days_skipped']
            )
    
    # Сохраняем новую заявку пользователя (перезаписываем старую)
    schedule_manager.save_request(
        employee_name, user_id, next_week_start,
        days_to_request, days_to_skip
    )
    
    # Формируем сообщение
    message_text = f"✅ Ваши дни на следующую неделю сохранены:\n\n"
    
    if guaranteed_days:
        guaranteed_days_short = [day_to_short(d) for d in guaranteed_days]
        message_text += f"✅ Гарантированные дни: {', '.join(guaranteed_days_short)}\n"
    
    if additional_days:
        additional_days_short = [day_to_short(d) for d in additional_days]
        message_text += f"📝 Дополнительно запрошены: {', '.join(additional_days_short)}\n"
    
    if days_to_skip:
        skipped_days_short = [day_to_short(d) for d in days_to_skip]
        message_text += f"⏭️ Пропущены: {', '.join(skipped_days_short)}\n"
    
    message_text += f"\nФинальное расписание будет отправлено в воскресенье вечером."
    
    await message.reply(message_text)
    log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/set_week_days", message_text)


@dp.message(Command("my_schedule"))
async def cmd_my_schedule(message: Message):
    """Показать расписание сотрудника на текущую неделю"""
    user_id = message.from_user.id
    user_info = get_user_info(message)
    
    if not employee_manager.is_registered(user_id):
        response = "Вы не зарегистрированы. Используйте /start"
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/my_schedule", response)
        return
    
    # Проверяем, был ли пользователь добавлен админом
    if not employee_manager.was_added_by_admin(user_id):
        response = (
            "❌ Для использования этой команды необходимо, чтобы администратор добавил вас в систему.\n\n"
            "Обратитесь к администратору для получения доступа."
        )
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/my_schedule", response)
        return
    
    employee_name = employee_manager.get_employee_name(user_id)
    if not employee_name:
        response = "Ошибка: не найдено ваше имя в системе"
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/my_schedule", response)
        return
    
    # Получаем начало текущей недели
    now = datetime.now(timezone)
    current_week_start = schedule_manager.get_week_start(now)
    
    # Проверяем, есть ли уже сохраненные расписания для текущей недели
    has_saved_schedules = schedule_manager.has_saved_schedules_for_week(current_week_start)
    week_dates = schedule_manager.get_week_dates(current_week_start)
    
    if has_saved_schedules:
        # Используем сохраненные расписания (load_schedule_for_date вернет default_schedule для дат без сохраненных данных)
        schedule = {}
        for date, day_name in week_dates:
            day_schedule = schedule_manager.load_schedule_for_date(date, employee_manager)
            schedule[day_name] = day_schedule.get(day_name, [])
    else:
        # Загружаем заявки на неделю и строим расписание с учетом заявок
        requests = schedule_manager.load_requests_for_week(current_week_start)
        schedule = schedule_manager.build_schedule_from_requests(current_week_start, requests, employee_manager)
    
    # Загружаем default_schedule для определения реальных мест
    default_schedule = schedule_manager.load_default_schedule()
    
    # Получаем расписание сотрудника из построенного расписания с местами
    employee_schedule = {}
    employee_places = {}  # Словарь для хранения мест сотрудника
    formatted_name = employee_manager.format_employee_name(employee_name)
    plain_name = employee_name  # Простое имя без форматирования
    
    for date, day_name in week_dates:
        employees = schedule.get(day_name, [])
        employee_schedule[day_name] = formatted_name in employees
        # Находим место сотрудника (если он в офисе)
        if formatted_name in employees:
            # Ищем место в default_schedule
            place = None
            if day_name in default_schedule:
                places_dict = default_schedule[day_name]
                for place_key, name in places_dict.items():
                    # Сравниваем простые имена
                    plain_name_in_schedule = schedule_manager.get_plain_name_from_formatted(name)
                    if plain_name_in_schedule == plain_name:
                        place = place_key
                        break
            
            # Если место не найдено, используем порядковый номер
            if place is None:
                try:
                    place_index = employees.index(formatted_name) + 1
                    place = f"1.{place_index}"
                except ValueError:
                    place = "?"
            
            employee_places[day_name] = place
        else:
            employee_places[day_name] = None
    
    # Форматируем сообщение с местами
    week_str = f"{week_dates[0][0].strftime('%d.%m')} - {week_dates[-1][0].strftime('%d.%m.%Y')}"
    
    office_days = [day for day, in_office in employee_schedule.items() if in_office]
    remote_days = [day for day, in_office in employee_schedule.items() if not in_office]
    
    message_text = f"📅 Ваше расписание на неделю {week_str}:\n\n"
    
    if office_days:
        office_days_with_places = []
        for day in office_days:
            place = employee_places.get(day)
            day_short = day_to_short(day)
            if place:
                office_days_with_places.append(f"{day_short} (место {place})")
            else:
                office_days_with_places.append(day_short)
        message_text += f"🏢 Дни в офисе: {', '.join(office_days_with_places)}\n"
    
    if remote_days:
        remote_days_short = [day_to_short(day) for day in remote_days]
        message_text += f"🏠 Дни удаленно: {', '.join(remote_days_short)}\n"
    
    keyboard = get_main_keyboard(user_id)
    await message.reply(message_text, reply_markup=keyboard)
    log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/my_schedule", message_text)


async def process_skip_day(date: datetime, employee_name: str, user_id: int, employee_manager, schedule_manager, notification_manager, bot, timezone):
    """Обработать пропуск одного дня для сотрудника"""
    now = datetime.now(timezone)
    
    # Проверяем, не прошел ли день
    if date.date() < now.date():
        return f"❌ Нельзя пропустить день {date.strftime('%d.%m.%Y')}, который уже прошел"
    
    # Получаем начало недели для указанной даты
    week_start = schedule_manager.get_week_start(date)
    current_week_start = schedule_manager.get_week_start(now)
    
    # Определяем день недели
    week_dates = schedule_manager.get_week_dates(week_start)
    day_name = None
    for d, day_n in week_dates:
        if d.date() == date.date():
            day_name = day_n
            break
    
    if not day_name:
        return f"❌ Дата {date.strftime('%d.%m.%Y')} не является рабочим днем (Пн-Пт)"
    
    # Если это текущая неделя - обновляем сохраненное расписание
    if week_start == current_week_start:
        # Проверяем, находится ли пользователь в очереди
        queue = schedule_manager.get_queue_for_date(date)
        in_queue = any(
            entry['employee_name'] == employee_name and entry['telegram_id'] == user_id
            for entry in queue
        )
        
        if in_queue:
            # Пользователь в очереди - удаляем из очереди
            schedule_manager.remove_from_queue(date, employee_name, user_id)
            return f"✅ Удалены из очереди на {day_name} ({date.strftime('%d.%m.%Y')})"
        else:
            # Пользователь в расписании - удаляем из расписания
            success, free_slots = schedule_manager.update_schedule_for_date(
                date, employee_name, 'remove', employee_manager
            )
            
            if success:
                # Обрабатываем очередь - добавляем первого, если есть место
                added_from_queue = schedule_manager.process_queue_for_date(date, employee_manager)
                
                if added_from_queue:
                    # Уведомляем добавленного из очереди
                    formatted_name = employee_manager.format_employee_name(added_from_queue['employee_name'])
                    try:
                        await bot.send_message(
                            added_from_queue['telegram_id'],
                            f"✅ Место освободилось!\n\n"
                            f"📅 {day_to_short(day_name)} ({date.strftime('%d.%m.%Y')})\n"
                            f"Вы автоматически добавлены в расписание."
                        )
                    except Exception as e:
                        logger.error(f"Ошибка отправки уведомления {added_from_queue['telegram_id']}: {e}")
                    
                    # Обновляем количество свободных мест после добавления из очереди
                    schedule = schedule_manager.load_schedule_for_date(date, employee_manager)
                    employees = schedule.get(day_name, [])
                    free_slots = MAX_OFFICE_SEATS - len(employees)
                
                # Уведомляем других сотрудников о свободном месте (если оно еще есть)
                if free_slots > 0:
                    await notification_manager.notify_available_slot(date, day_name, free_slots)
                
                if added_from_queue:
                    return f"✅ Удалены из расписания на {day_name} ({date.strftime('%d.%m.%Y')})\n💡 Место занято сотрудником из очереди. 🆓 Свободных мест: {free_slots}"
                else:
                    return f"✅ Удалены из расписания на {day_name} ({date.strftime('%d.%m.%Y')})\n💡 Освобождено место. Другие сотрудники получили уведомление."
            else:
                return f"❌ Ошибка при обновлении расписания на {date.strftime('%d.%m.%Y')}"
    else:
        # Это следующая неделя - работаем с заявками
        # Загружаем существующие заявки
        requests = schedule_manager.load_requests_for_week(week_start)
        
        # Ищем заявку сотрудника
        user_request = None
        for req in requests:
            if req['employee_name'] == employee_name and req['telegram_id'] == user_id:
                user_request = req
                break
        
        # Если заявки нет, создаем новую
        if not user_request:
            days_requested = []
            days_skipped = [day_name]
        else:
            # Обновляем существующую заявку
            days_requested = user_request['days_requested'].copy()
            days_skipped = user_request['days_skipped'].copy()
            
            if day_name not in days_skipped:
                days_skipped.append(day_name)
            # Удаляем из запрошенных, если был там
            if day_name in days_requested:
                days_requested.remove(day_name)
        
        # Очищаем старые заявки и пересохраняем все
        schedule_manager.clear_requests_for_week(week_start)
        for req in requests:
            if req['employee_name'] != employee_name or req['telegram_id'] != user_id:
                schedule_manager.save_request(
                    req['employee_name'], req['telegram_id'], week_start,
                    req['days_requested'], req['days_skipped']
                )
        # Сохраняем обновленную заявку сотрудника
        schedule_manager.save_request(employee_name, user_id, week_start, days_requested, days_skipped)
        
        return f"✅ День {day_name} ({date.strftime('%d.%m.%Y')}) добавлен в список пропусков на следующую неделю"


@dp.message(Command("skip_day"))
async def cmd_skip_day(message: Message):
    """Пропустить день (можно указать несколько дат через пробел)"""
    user_id = message.from_user.id
    user_info = get_user_info(message)
    
    if not employee_manager.is_registered(user_id):
        response = "Вы не зарегистрированы. Используйте /start"
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/skip_day", response)
        return
    
    # Проверяем, был ли пользователь добавлен админом
    if not employee_manager.was_added_by_admin(user_id):
        response = (
            "❌ Для использования этой команды необходимо, чтобы администратор добавил вас в систему.\n\n"
            "Обратитесь к администратору для получения доступа."
        )
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/skip_day", response)
        return
    
    employee_name = employee_manager.get_employee_name(user_id)
    if not employee_name:
        response = "Ошибка: не найдено ваше имя в системе"
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/skip_day", response)
        return
    
    # Парсим даты из команды
    command_parts = message.text.split()
    if len(command_parts) < 2:
        response = "Укажите дату(ы). Например: /skip_day 2024-12-20 или /skip_day 2024-12-20 2024-12-21"
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/skip_day", response)
        return
    
    # Парсим все даты
    dates = []
    for date_str in command_parts[1:]:
        try:
            date = datetime.strptime(date_str, "%Y-%m-%d")
            date = timezone.localize(date)
            dates.append(date)
        except ValueError:
            response = f"Неверный формат даты: {date_str}. Используйте формат: YYYY-MM-DD"
            await message.reply(response)
            log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/skip_day", response)
            return
    
    # Обрабатываем каждую дату
    results = []
    for date in dates:
        result = await process_skip_day(date, employee_name, user_id, employee_manager, schedule_manager, notification_manager, bot, timezone)
        results.append(result)
    
    # Формируем ответ
    message_text = "\n\n".join(results)
    await message.reply(message_text)
    log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/skip_day", message_text)


async def process_add_day(date: datetime, employee_name: str, user_id: int, employee_manager, schedule_manager, timezone):
    """Обработать добавление одного дня для сотрудника"""
    now = datetime.now(timezone)
    
    # Проверяем, не прошел ли день
    if date.date() < now.date():
        return f"❌ Нельзя добавить день {date.strftime('%d.%m.%Y')}, который уже прошел"
    
    # Получаем начало недели для указанной даты
    week_start = schedule_manager.get_week_start(date)
    current_week_start = schedule_manager.get_week_start(now)
    
    # Определяем день недели
    week_dates = schedule_manager.get_week_dates(week_start)
    day_name = None
    for d, day_n in week_dates:
        if d.date() == date.date():
            day_name = day_n
            break
    
    if not day_name:
        return f"❌ Дата {date.strftime('%d.%m.%Y')} не является рабочим днем (Пн-Пт)"
    
    # Если это текущая неделя - обновляем сохраненное расписание
    if week_start == current_week_start:
        success, free_slots = schedule_manager.update_schedule_for_date(
            date, employee_name, 'add', employee_manager
        )
        
        if success:
            # Удаляем из очереди, если был там
            schedule_manager.remove_from_queue(date, employee_name, user_id)
            return f"✅ Добавлены в расписание на {day_name} ({date.strftime('%d.%m.%Y')})\n💡 Свободных мест осталось: {free_slots}"
        else:
            # Все места заняты - добавляем в очередь
            added_to_queue = schedule_manager.add_to_queue(date, employee_name, user_id)
            
            if added_to_queue:
                queue = schedule_manager.get_queue_for_date(date)
                position = 1
                # Находим позицию в очереди
                for i, entry in enumerate(queue):
                    if entry['employee_name'] == employee_name and entry['telegram_id'] == user_id:
                        position = i + 1
                        break
                
                return f"⏳ Все места заняты. Добавлены в очередь на {day_name} ({date.strftime('%d.%m.%Y')})\n📋 Позиция в очереди: {position}\n\nКогда место освободится, вы автоматически будете добавлены в расписание."
            else:
                return f"❌ Уже в очереди на {day_name} ({date.strftime('%d.%m.%Y')})"
    else:
        # Это следующая неделя - работаем с заявками
        # Загружаем существующие заявки
        requests = schedule_manager.load_requests_for_week(week_start)
        
        # Ищем заявку сотрудника
        user_request = None
        for req in requests:
            if req['employee_name'] == employee_name and req['telegram_id'] == user_id:
                user_request = req
                break
        
        # Если заявки нет, создаем новую
        if not user_request:
            days_requested = [day_name]
            days_skipped = []
        else:
            # Обновляем существующую заявку
            days_requested = user_request['days_requested'].copy()
            days_skipped = user_request['days_skipped'].copy()
            
            if day_name not in days_requested:
                days_requested.append(day_name)
            # Удаляем из пропусков, если был там
            if day_name in days_skipped:
                days_skipped.remove(day_name)
        
        # Очищаем старые заявки и пересохраняем все
        schedule_manager.clear_requests_for_week(week_start)
        for req in requests:
            if req['employee_name'] != employee_name or req['telegram_id'] != user_id:
                schedule_manager.save_request(
                    req['employee_name'], req['telegram_id'], week_start,
                    req['days_requested'], req['days_skipped']
                )
        # Сохраняем обновленную заявку сотрудника
        schedule_manager.save_request(employee_name, user_id, week_start, days_requested, days_skipped)
        
        return f"✅ День {day_name} ({date.strftime('%d.%m.%Y')}) добавлен в список запрошенных дней на следующую неделю"


@dp.message(Command("add_day"))
async def cmd_add_day(message: Message):
    """Запросить дополнительный день (можно указать несколько дат через пробел)"""
    user_id = message.from_user.id
    user_info = get_user_info(message)
    
    if not employee_manager.is_registered(user_id):
        response = "Вы не зарегистрированы. Используйте /start"
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/add_day", response)
        return
    
    # Проверяем, был ли пользователь добавлен админом
    if not employee_manager.was_added_by_admin(user_id):
        response = (
            "❌ Для использования этой команды необходимо, чтобы администратор добавил вас в систему.\n\n"
            "Обратитесь к администратору для получения доступа."
        )
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/add_day", response)
        return
    
    employee_name = employee_manager.get_employee_name(user_id)
    if not employee_name:
        response = "Ошибка: не найдено ваше имя в системе"
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/add_day", response)
        return
    
    # Парсим даты из команды
    command_parts = message.text.split()
    if len(command_parts) < 2:
        response = "Укажите дату(ы). Например: /add_day 2024-12-20 или /add_day 2024-12-20 2024-12-21"
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/add_day", response)
        return
    
    # Парсим все даты
    dates = []
    for date_str in command_parts[1:]:
        try:
            date = datetime.strptime(date_str, "%Y-%m-%d")
            date = timezone.localize(date)
            dates.append(date)
        except ValueError:
            response = f"Неверный формат даты: {date_str}. Используйте формат: YYYY-MM-DD"
            await message.reply(response)
            log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/add_day", response)
            return
    
    # Обрабатываем каждую дату
    results = []
    for date in dates:
        result = await process_add_day(date, employee_name, user_id, employee_manager, schedule_manager, timezone)
        results.append(result)
    
    # Формируем ответ
    message_text = "\n\n".join(results)
    await message.reply(message_text)
    log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/add_day", message_text)


@dp.message(Command("full_schedule"))
async def cmd_full_schedule(message: Message):
    """Показать полное расписание на дату (только для админов)"""
    user_id = message.from_user.id
    user_info = get_user_info(message)
    
    if not admin_manager.is_admin(user_id):
        response = "Эта команда доступна только администраторам"
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/full_schedule", response)
        return
    
    # Парсим дату из команды
    command_parts = message.text.split()
    if len(command_parts) > 1:
        try:
            date = datetime.strptime(command_parts[1], "%Y-%m-%d")
            date = timezone.localize(date)
        except:
            response = "Неверный формат даты. Используйте: /full_schedule 2024-12-20"
            await message.reply(response)
            log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/full_schedule", response)
            return
    else:
        date = datetime.now(timezone)
    
    # Получаем начало недели для указанной даты
    week_start = schedule_manager.get_week_start(date)
    
    # Проверяем, есть ли уже сохраненные расписания для этой недели
    has_saved_schedules = schedule_manager.has_saved_schedules_for_week(week_start)
    week_dates = schedule_manager.get_week_dates(week_start)
    
    if has_saved_schedules:
        # Используем сохраненные расписания (load_schedule_for_date вернет default_schedule для дат без сохраненных данных)
        schedule = {}
        for d, day_name in week_dates:
            day_schedule = schedule_manager.load_schedule_for_date(d, employee_manager)
            schedule[day_name] = day_schedule.get(day_name, [])
    else:
        # Загружаем заявки на неделю и строим расписание с учетом заявок
        requests = schedule_manager.load_requests_for_week(week_start)
        schedule = schedule_manager.build_schedule_from_requests(week_start, requests, employee_manager)
    
    # Загружаем default_schedule для определения реальных мест
    default_schedule = schedule_manager.load_default_schedule()
    
    message_text = f"📅 Расписание на {date.strftime('%d.%m.%Y')}:\n\n"
    message_text += format_schedule_with_places(schedule, default_schedule)
    
    keyboard = get_main_keyboard(user_id)
    await message.reply(message_text, reply_markup=keyboard)
    log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/full_schedule", message_text[:200])


@dp.message(Command("admin_add_employee"))
async def cmd_admin_add_employee(message: Message):
    """Добавить сотрудника (только для админов)"""
    user_id = message.from_user.id
    user_info = get_user_info(message)
    
    if not admin_manager.is_admin(user_id):
        response = "Эта команда доступна только администраторам"
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_add_employee", response)
        return
    
    # Проверяем, есть ли reply на сообщение пользователя
    telegram_id = None
    username = None
    
    # Если есть reply на сообщение, получаем ID из него
    if message.reply_to_message and message.reply_to_message.from_user:
        telegram_id = message.reply_to_message.from_user.id
        username = message.reply_to_message.from_user.username
    
    # Если нет reply, проверяем entities (упоминания пользователей)
    if not telegram_id and message.entities:
        for entity in message.entities:
            if entity.type == "text_mention" and entity.user:
                # Прямое упоминание пользователя
                telegram_id = entity.user.id
                username = entity.user.username or entity.user.first_name
                break
    
    # Парсим команду - ищем username в тексте (всегда начинается с @)
    text = message.text
    username_in_text = None
    username_start = text.find('@')
    
    if username_start != -1:
        # Нашли @, извлекаем username
        username_part = text[username_start:].split()[0]  # Берем первое слово после @
        username_in_text = username_part.lstrip('@')
        # Удаляем username из текста для извлечения имени
        text_without_username = text[:username_start].strip()
    else:
        text_without_username = text
    
    # Извлекаем имя - всё после команды до username или до конца
    command_parts = text_without_username.split(maxsplit=1)
    if len(command_parts) < 2:
        response = (
            "Используйте один из форматов:\n\n"
            "1. Ответьте на сообщение пользователя:\n"
            "   /admin_add_employee [имя]\n\n"
            "2. Укажите username:\n"
            "   /admin_add_employee [имя] @username\n\n"
            "3. Укажите telegram_id (если знаете):\n"
            "   /admin_add_employee [имя] [telegram_id]\n\n"
            "4. Укажите telegram_id и username:\n"
            "   /admin_add_employee [имя] [telegram_id] @username"
        )
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_add_employee", response)
        return
    
    name = command_parts[1].strip()
    
    # Если не нашли через reply или entities, пытаемся парсить из текста
    if not telegram_id:
        if username_in_text:
            # Есть username в тексте
            username = username_in_text
            
            # Проверяем, не используется ли имя другим сотрудником
            existing_id = employee_manager.get_employee_id(name)
            if existing_id:
                # Имя уже используется другим сотрудником
                existing_employee_data = employee_manager.get_employee_data(existing_id)
                existing_username = ""
                if existing_employee_data:
                    _, _, existing_username = existing_employee_data
                username_display = f" (@{existing_username})" if existing_username else ""
                response = (
                    f"❌ Имя '{name}' уже используется другим сотрудником.\n\n"
                    f"Текущий владелец имени:\n"
                    f"• Telegram ID: {existing_id}{username_display}\n\n"
                    f"💡 Используйте другое имя для нового сотрудника."
                )
                await message.reply(response)
                log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_add_employee", response)
                return
            
            # Сохраняем отложенную запись для использования при /start
            was_existing, old_name = employee_manager.add_pending_employee(username, name)
            if was_existing:
                # Запись уже существовала - обновлено имя
                response = (
                    f"ℹ️ Отложенная запись для @{username} уже существовала.\n\n"
                    f"📝 Имя обновлено: '{old_name}' → '{name}'\n\n"
                    f"Попросите @{username} написать боту /start - он будет автоматически добавлен с именем '{name}'."
                )
            else:
                # Новая запись
                response = (
                    f"✅ Отложенная запись для сотрудника {name} (@{username}) сохранена.\n\n"
                    f"Попросите @{username} написать боту /start - он будет автоматически добавлен с именем '{name}'."
                )
            await message.reply(response)
            log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_add_employee", response)
            return
        else:
            # Проверяем, может быть указан ID после имени
            remaining_parts = text_without_username.split()
            if len(remaining_parts) >= 3:
                # Пытаемся понять, это ID или что-то еще
                try:
                    telegram_id = int(remaining_parts[2])
                except (ValueError, IndexError):
                    response = (
                        "Укажите username или ответьте на сообщение пользователя:\n"
                        "/admin_add_employee [имя] @username"
                    )
                    await message.reply(response)
                    log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_add_employee", response)
                    return
            else:
                response = (
                    "Укажите username или ответьте на сообщение пользователя:\n"
                    "/admin_add_employee [имя] @username"
                )
                await message.reply(response)
                log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_add_employee", response)
                return
    
    # Если у нас есть ID, добавляем сотрудника
    if telegram_id:
        # Получаем имя из Telegram, если есть reply
        telegram_name = None
        if message.reply_to_message and message.reply_to_message.from_user:
            telegram_name = message.reply_to_message.from_user.first_name or name
            # Если username не был получен ранее, берем из reply
            if not username:
                username = message.reply_to_message.from_user.username
        
        # Если имя из Telegram не получено, проверяем, зарегистрирован ли пользователь
        if not telegram_name:
            # Если пользователь уже зарегистрирован через /start, берем его имя из базы
            employee_data = employee_manager.get_employee_data(telegram_id)
            if employee_data:
                _, telegram_name, existing_username = employee_data
                # Сохраняем существующий username, если новый не указан
                if not username:
                    username = existing_username
            else:
                # Если не зарегистрирован, используем имя вручную как имя из Telegram
                telegram_name = name
        
        # Если username не был получен, используем найденный в тексте
        if not username and username_in_text:
            username = username_in_text
        
        # Если username все еще не получен, берем из существующих данных сотрудника
        if not username:
            employee_data = employee_manager.get_employee_data(telegram_id)
            if employee_data:
                _, _, existing_username = employee_data
                username = existing_username
        
        # Проверяем, есть ли отложенная запись для этого username, и используем имя из неё
        if username:
            pending_name = employee_manager.get_pending_employee(username)
            if pending_name:
                # Используем имя из отложенной записи, если оно было указано админом
                name = pending_name
                # Удаляем отложенную запись, так как пользователь теперь добавлен
                employee_manager.remove_pending_employee(username)
        
        # Проверяем, не используется ли имя другим сотрудником
        existing_id = employee_manager.get_employee_id(name)
        if existing_id and existing_id != telegram_id:
            # Имя уже используется другим сотрудником
            existing_employee_data = employee_manager.get_employee_data(existing_id)
            existing_username = ""
            if existing_employee_data:
                _, _, existing_username = existing_employee_data
            username_display = f" (@{existing_username})" if existing_username else ""
            response = (
                f"❌ Имя '{name}' уже используется другим сотрудником.\n\n"
                f"Текущий владелец имени:\n"
                f"• Telegram ID: {existing_id}{username_display}\n\n"
                f"💡 Используйте другое имя для нового сотрудника."
            )
            await message.reply(response)
            log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_add_employee", response)
            return
        
        if employee_manager.add_employee(name, telegram_id, telegram_name, username):
            # Обновляем имя в default_schedule.txt, если сотрудник там есть
            formatted_name = employee_manager.format_employee_name_by_id(telegram_id)
            schedule_manager.update_employee_name_in_default_schedule(name, formatted_name)
            
            username_display = f" (@{username})" if username else ""
            response = (
                f"✅ Сотрудник {name}{username_display} добавлен\n"
                f"Telegram ID: {telegram_id}"
            )
            await message.reply(response)
            log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_add_employee", response)
        else:
            # Этот случай не должен произойти, так как мы уже проверили выше
            existing_id = employee_manager.get_employee_id(name)
            response = (
                f"❌ Сотрудник {name} уже существует\n"
                f"Текущий Telegram ID: {existing_id}"
            )
            await message.reply(response)
            log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_add_employee", response)
    else:
        response = "Не удалось определить Telegram ID пользователя"
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_add_employee", response)


@dp.message(Command("admin_add_admin"))
async def cmd_admin_add_admin(message: Message):
    """Добавить администратора (только для админов)"""
    user_id = message.from_user.id
    user_info = get_user_info(message)
    
    if not admin_manager.is_admin(user_id):
        response = "Эта команда доступна только администраторам"
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_add_admin", response)
        return
    
    # Проверяем, есть ли reply на сообщение пользователя
    telegram_id = None
    username = None
    
    # Если есть reply на сообщение, получаем ID из него
    if message.reply_to_message and message.reply_to_message.from_user:
        telegram_id = message.reply_to_message.from_user.id
        username = message.reply_to_message.from_user.username
    
    # Если нет reply, проверяем entities (упоминания пользователей)
    if not telegram_id and message.entities:
        for entity in message.entities:
            if entity.type == "text_mention" and entity.user:
                telegram_id = entity.user.id
                username = entity.user.username or entity.user.first_name
                break
    
    # Парсим команду
    command_parts = message.text.split(maxsplit=1)
    
    if not telegram_id:
        if len(command_parts) >= 2:
            username_or_id = command_parts[1].lstrip('@')
            try:
                telegram_id = int(username_or_id)
            except ValueError:
                # Это username - ищем в employees.txt
                username = username_or_id
                found_id = employee_manager.get_telegram_id_by_username(username)
                if found_id:
                    telegram_id = found_id
                else:
                    response = (
                        f"❌ Пользователь @{username} не найден в списке сотрудников.\n\n"
                        f"Сначала добавьте сотрудника командой:\n"
                        f"/admin_add_employee [имя] @{username}\n\n"
                        f"Или используйте один из способов:\n"
                        f"1. Ответьте на сообщение пользователя командой:\n"
                        f"   /admin_add_admin\n\n"
                        f"2. Укажите telegram_id:\n"
                        f"   /admin_add_admin [telegram_id]"
                    )
                    await message.reply(response)
                    log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_add_admin", response)
                    return
        else:
            response = (
                "Используйте один из форматов:\n\n"
                "1. Ответьте на сообщение пользователя:\n"
                "   /admin_add_admin\n\n"
                "2. Укажите username (никнейм в Telegram):\n"
                "   /admin_add_admin @username\n\n"
                "3. Укажите telegram_id:\n"
                "   /admin_add_admin [telegram_id]"
            )
            await message.reply(response)
            log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_add_admin", response)
            return
    
    # Если у нас есть ID, добавляем администратора
    if telegram_id:
        if admin_manager.add_admin(telegram_id):
            username_display = f" (@{username})" if username else ""
            response = (
                f"✅ Администратор{username_display} добавлен\n"
                f"Telegram ID: {telegram_id}"
            )
            await message.reply(response)
            log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_add_admin", response)
        else:
            response = (
                f"❌ Пользователь уже является администратором\n"
                f"Telegram ID: {telegram_id}"
            )
            await message.reply(response)
            log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_add_admin", response)
    else:
        response = "Не удалось определить Telegram ID пользователя"
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_add_admin", response)


@dp.message(Command("admin_list_admins"))
async def cmd_admin_list_admins(message: Message):
    """Показать список администраторов (только для админов)"""
    user_id = message.from_user.id
    user_info = get_user_info(message)
    
    if not admin_manager.is_admin(user_id):
        response = "Эта команда доступна только администраторам"
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_list_admins", response)
        return
    
    admins = admin_manager.get_all_admins()
    
    if not admins:
        response = "Список администраторов пуст"
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_list_admins", response)
        return
    
    message_text = "👑 Список администраторов:\n\n"
    for admin_id in admins:
        # Получаем данные сотрудника для получения username
        employee_data = employee_manager.get_employee_data(admin_id)
        if employee_data:
            _, _, username = employee_data
            if username:
                message_text += f"• {admin_id} (@{username})\n"
            else:
                message_text += f"• {admin_id}\n"
        else:
            message_text += f"• {admin_id}\n"
    
    await message.reply(message_text)
    log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_list_admins", message_text)


@dp.message(Command("admin_test_schedule"))
async def cmd_admin_test_schedule(message: Message):
    """Тестовая команда для отправки расписания (только для админов)"""
    user_id = message.from_user.id
    user_info = get_user_info(message)
    
    if not admin_manager.is_admin(user_id):
        response = "Эта команда доступна только администраторам"
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_test_schedule", response)
        return
    
    response = "📤 Начинаю рассылку расписания на следующую неделю..."
    await message.reply(response)
    log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_test_schedule", response)
    
    try:
        await notification_manager.send_weekly_schedule(admins_only=True)
        response = "✅ Расписание успешно отправлено администраторам"
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_test_schedule", response)
    except Exception as e:
        response = f"❌ Ошибка при отправке расписания: {e}"
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_test_schedule", response)


@dp.message(Command("admin_set_default_schedule"))
async def cmd_admin_set_default_schedule(message: Message):
    """Установить расписание по умолчанию для дня (только для админов)"""
    user_id = message.from_user.id
    user_info = get_user_info(message)
    
    if not admin_manager.is_admin(user_id):
        response = "Эта команда доступна только администраторам"
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_set_default_schedule", response)
        return
    
    # Парсим команду: /admin_set_default_schedule [день] [список сотрудников через запятую]
    command_parts = message.text.split(maxsplit=2)
    if len(command_parts) < 3:
        response = (
            "Используйте формат:\n"
            "/admin_set_default_schedule [день] [список сотрудников]\n\n"
            "Пример:\n"
            "/admin_set_default_schedule Понедельник Вася, Дима Ч, Айлар, Егор, Илья, Даша, Виталий, Тимур\n\n"
            "Дни: Понедельник, Вторник, Среда, Четверг, Пятница"
        )
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_set_default_schedule", response)
        return
    
    day_name = command_parts[1].strip()
    employees_str = command_parts[2].strip()
    
    # Проверяем, что день недели корректен
    valid_days = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница']
    if day_name not in valid_days:
        response = f"❌ Неверный день недели: {day_name}\n\nДопустимые дни: {', '.join(valid_days)}"
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_set_default_schedule", response)
        return
    
    # Парсим список сотрудников
    employees = [e.strip() for e in employees_str.split(',') if e.strip()]
    
    if not employees:
        response = "❌ Список сотрудников не может быть пустым"
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_set_default_schedule", response)
        return
    
    # Проверяем количество мест
    if len(employees) > MAX_OFFICE_SEATS:
        response = f"❌ Слишком много сотрудников: {len(employees)}. Максимум: {MAX_OFFICE_SEATS}"
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_set_default_schedule", response)
        return
    
    # Загружаем текущее расписание по умолчанию
    default_schedule = schedule_manager.load_default_schedule()
    
    # Конвертируем список сотрудников в формат словаря мест (подразделение.место)
    # Всегда создаем все 8 мест, даже если указано меньше сотрудников
    places_dict = {}
    for i in range(1, MAX_OFFICE_SEATS + 1):
        place_key = f'1.{i}'
        if i <= len(employees):
            places_dict[place_key] = employees[i - 1]
        else:
            # Создаем пустое место
            places_dict[place_key] = ''
    
    # Обновляем расписание для указанного дня
    default_schedule[day_name] = places_dict
    
    # Сохраняем обновленное расписание
    schedule_manager.save_default_schedule(default_schedule)
    
    # Форматируем имена сотрудников для ответа
    formatted_employees = [employee_manager.format_employee_name(emp) for emp in employees]
    
    response = (
        f"✅ Расписание по умолчанию для {day_name} обновлено:\n\n"
        f"{', '.join(formatted_employees)}\n\n"
        f"Всего сотрудников: {len(employees)}"
    )
    await message.reply(response)
    log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_set_default_schedule", response)


@dp.message(Command("admin_refresh_schedules"))
async def cmd_admin_refresh_schedules(message: Message):
    """Обновить имена сотрудников в schedules и default_schedule (только для админов)"""
    user_id = message.from_user.id
    user_info = get_user_info(message)
    
    if not admin_manager.is_admin(user_id):
        response = "Эта команда доступна только администраторам"
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_refresh_schedules", response)
        return
    
    response = "🔄 Начинаю обновление расписаний..."
    await message.reply(response)
    log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_refresh_schedules", response)
    
    try:
        # Перезагружаем данные сотрудников из Google Sheets
        employee_manager.reload_employees()
        
        # Обновляем расписания
        updated_default, updated_schedules = schedule_manager.refresh_all_schedules_with_usernames()
        
        response = (
            f"✅ Обновление завершено:\n\n"
            f"📋 default_schedule: обновлено {updated_default} записей\n"
            f"📅 schedules: обновлено {updated_schedules} записей\n\n"
            f"Все имена сотрудников синхронизированы с данными из таблицы employees."
        )
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_refresh_schedules", response)
    except Exception as e:
        response = f"❌ Ошибка при обновлении расписаний: {e}"
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_refresh_schedules", response)


@dp.message(Command("admin_skip_day"))
async def cmd_admin_skip_day(message: Message):
    """Пропустить день для сотрудника (только для админов, можно указать несколько дат)"""
    user_id = message.from_user.id
    user_info = get_user_info(message)
    
    if not admin_manager.is_admin(user_id):
        response = "Эта команда доступна только администраторам"
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_skip_day", response)
        return
    
    # Парсим команду: /admin_skip_day @username date1 date2 ...
    command_parts = message.text.split()
    if len(command_parts) < 3:
        response = (
            "Используйте формат:\n"
            "/admin_skip_day @username 2024-12-20\n"
            "или\n"
            "/admin_skip_day @username 2024-12-20 2024-12-21"
        )
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_skip_day", response)
        return
    
    # Ищем username (начинается с @)
    username = None
    date_start_idx = 1
    for i, part in enumerate(command_parts[1:], 1):
        if part.startswith('@'):
            username = part.lstrip('@')
            date_start_idx = i + 1
            break
    
    if not username:
        response = "Укажите username сотрудника (начинается с @). Например: /admin_skip_day @username 2024-12-20"
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_skip_day", response)
        return
    
    # Находим telegram_id по username
    target_telegram_id = employee_manager.get_telegram_id_by_username(username)
    if not target_telegram_id:
        response = f"❌ Сотрудник @{username} не найден в системе"
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_skip_day", response)
        return
    
    target_employee_name = employee_manager.get_employee_name(target_telegram_id)
    if not target_employee_name:
        response = f"❌ Не найдено имя сотрудника @{username}"
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_skip_day", response)
        return
    
    # Парсим даты
    dates = []
    for date_str in command_parts[date_start_idx:]:
        try:
            date = datetime.strptime(date_str, "%Y-%m-%d")
            date = timezone.localize(date)
            dates.append(date)
        except ValueError:
            response = f"Неверный формат даты: {date_str}. Используйте формат: YYYY-MM-DD"
            await message.reply(response)
            log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_skip_day", response)
            return
    
    # Обрабатываем каждую дату
    results = []
    for date in dates:
        result = await process_skip_day(date, target_employee_name, target_telegram_id, employee_manager, schedule_manager, notification_manager, bot, timezone)
        results.append(result)
    
    # Формируем ответ
    message_text = f"👤 Сотрудник: @{username}\n\n" + "\n\n".join(results)
    await message.reply(message_text)
    log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_skip_day", message_text)


@dp.message(Command("admin_sync_from_sheets"))
async def cmd_admin_sync_from_sheets(message: Message):
    """Синхронизировать данные из Google Sheets в PostgreSQL (только для админов)"""
    user_id = message.from_user.id
    user_info = get_user_info(message)
    
    if not admin_manager.is_admin(user_id):
        response = "Эта команда доступна только администраторам"
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_sync_from_sheets", response)
        return
    
    response = "🔄 Начинаю синхронизацию данных из Google Sheets в PostgreSQL..."
    await message.reply(response)
    log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_sync_from_sheets", response)
    
    try:
        from config import USE_GOOGLE_SHEETS
        if not USE_GOOGLE_SHEETS:
            response = "❌ Google Sheets отключен"
            await message.reply(response)
            log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_sync_from_sheets", response)
            return
        
        # Импортируем функции синхронизации из check_and_sync_data.py
        from check_and_sync_data import (
            compare_and_sync_admins, compare_and_sync_employees, compare_and_sync_pending_employees,
            compare_and_sync_default_schedule, compare_and_sync_schedules, compare_and_sync_requests,
            compare_and_sync_queue
        )
        from google_sheets_manager import GoogleSheetsManager
        
        sheets_manager = GoogleSheetsManager()
        if not sheets_manager.is_available():
            response = "❌ Google Sheets недоступен"
            await message.reply(response)
            log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_sync_from_sheets", response)
            return
        
        # Проверяем PostgreSQL
        from database_sync import _get_connection
        conn = _get_connection()
        if not conn:
            response = "❌ PostgreSQL недоступен"
            await message.reply(response)
            log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_sync_from_sheets", response)
            return
        conn.close()
        
        # Выполняем синхронизацию
        changes = False
        changes |= compare_and_sync_admins(sheets_manager)
        changes |= compare_and_sync_employees(sheets_manager)
        changes |= compare_and_sync_pending_employees(sheets_manager)
        changes |= compare_and_sync_default_schedule(sheets_manager)
        changes |= compare_and_sync_schedules(sheets_manager)
        changes |= compare_and_sync_requests(sheets_manager)
        changes |= compare_and_sync_queue(sheets_manager)
        
        if changes:
            # Перезагружаем данные в менеджерах
            employee_manager.reload_employees()
            employee_manager.reload_pending_employees()
            admin_manager.reload_admins()
            schedule_manager.load_default_schedule()
            
            response = (
                "✅ Синхронизация завершена!\n\n"
                "Данные из Google Sheets успешно скопированы в PostgreSQL.\n"
                "Все менеджеры обновлены."
            )
        else:
            response = (
                "✅ Синхронизация завершена!\n\n"
                "Все данные идентичны. Изменений не требуется."
            )
        
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_sync_from_sheets", response)
    except Exception as e:
        response = f"❌ Ошибка при синхронизации: {e}"
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_sync_from_sheets", response)
        logger.error(f"Ошибка синхронизации из Google Sheets: {e}", exc_info=True)


@dp.message(Command("admin_add_day"))
async def cmd_admin_add_day(message: Message):
    """Добавить день для сотрудника (только для админов, можно указать несколько дат)"""
    user_id = message.from_user.id
    user_info = get_user_info(message)
    
    if not admin_manager.is_admin(user_id):
        response = "Эта команда доступна только администраторам"
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_add_day", response)
        return
    
    # Парсим команду: /admin_add_day @username date1 date2 ...
    command_parts = message.text.split()
    if len(command_parts) < 3:
        response = (
            "Используйте формат:\n"
            "/admin_add_day @username 2024-12-20\n"
            "или\n"
            "/admin_add_day @username 2024-12-20 2024-12-21"
        )
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_add_day", response)
        return
    
    # Ищем username (начинается с @)
    username = None
    date_start_idx = 1
    for i, part in enumerate(command_parts[1:], 1):
        if part.startswith('@'):
            username = part.lstrip('@')
            date_start_idx = i + 1
            break
    
    if not username:
        response = "Укажите username сотрудника (начинается с @). Например: /admin_add_day @username 2024-12-20"
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_add_day", response)
        return
    
    # Находим telegram_id по username
    target_telegram_id = employee_manager.get_telegram_id_by_username(username)
    if not target_telegram_id:
        response = f"❌ Сотрудник @{username} не найден в системе"
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_add_day", response)
        return
    
    target_employee_name = employee_manager.get_employee_name(target_telegram_id)
    if not target_employee_name:
        response = f"❌ Не найдено имя сотрудника @{username}"
        await message.reply(response)
        log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_add_day", response)
        return
    
    # Парсим даты
    dates = []
    for date_str in command_parts[date_start_idx:]:
        try:
            date = datetime.strptime(date_str, "%Y-%m-%d")
            date = timezone.localize(date)
            dates.append(date)
        except ValueError:
            response = f"Неверный формат даты: {date_str}. Используйте формат: YYYY-MM-DD"
            await message.reply(response)
            log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_add_day", response)
            return
    
    # Обрабатываем каждую дату
    results = []
    for date in dates:
        result = await process_add_day(date, target_employee_name, target_telegram_id, employee_manager, schedule_manager, timezone)
        results.append(result)
    
    # Формируем ответ
    message_text = f"👤 Сотрудник: @{username}\n\n" + "\n\n".join(results)
    await message.reply(message_text)
    log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "/admin_add_day", message_text)


# Обработка текстовых сообщений (для ответов на напоминания)
@dp.message()
async def handle_text_message(message: Message):
    """Обработка текстовых сообщений (ответы на напоминания)"""
    user_id = message.from_user.id
    
    if not employee_manager.is_registered(user_id):
        return
    
    # Проверяем, был ли пользователь добавлен админом
    if not employee_manager.was_added_by_admin(user_id):
        return  # Не обрабатываем текстовые сообщения от неодобренных пользователей
    
    # Если сообщение похоже на список дней недели
    text = message.text.lower()
    if any(day in text for day in WEEKDAYS_RU.keys()):
        # Парсим дни
        days = parse_weekdays(message.text)
        
        if days:
            employee_name = employee_manager.get_employee_name(user_id)
            if not employee_name:
                return
            
            # Получаем начало следующей недели
            now = datetime.now(timezone)
            next_week_start = schedule_manager.get_week_start(now + timedelta(days=7))
            
            # Определяем дни
            default_schedule = schedule_manager.load_default_schedule()
            days_to_skip = []
            days_to_request = []
            
            week_days = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница']
            for day in week_days:
                if day in default_schedule:
                    # Проверяем, есть ли сотрудник в расписании (новый формат: словарь мест)
                    places_dict = default_schedule[day]
                    employee_in_schedule = False
                    for place_key, emp in places_dict.items():
                        plain_name = schedule_manager.get_plain_name_from_formatted(emp)
                        if plain_name == employee_name:
                            employee_in_schedule = True
                            break
                    
                    if employee_in_schedule:
                        if day not in days:
                            days_to_skip.append(day)
                        else:
                            days_to_request.append(day)
                    else:
                        if day in days:
                            days_to_request.append(day)
            
            # Сохраняем заявку
            schedule_manager.save_request(
                employee_name, user_id, next_week_start,
                days_to_request, days_to_skip
            )
            
            user_info = get_user_info(message)
            response = (
                f"✅ Ваши дни на следующую неделю сохранены:\n"
                f"В офисе: {', '.join([day_to_short(d) for d in days])}\n\n"
                f"Финальное расписание будет отправлено в воскресенье вечером."
            )
            await message.reply(response)
            log_command(user_info['user_id'], user_info['username'], user_info['first_name'], "текстовое сообщение (дни недели)", response)


# Обработчики callback-запросов для кнопок
@dp.callback_query(lambda c: c.data.startswith("cmd_"))
async def handle_callback(callback: CallbackQuery):
    """Обработчик нажатий на кнопки"""
    user_id = callback.from_user.id
    username = callback.from_user.username
    first_name = callback.from_user.first_name or "Пользователь"
    command = callback.data
    
    logger.info(f"🔘 Callback получен: {command} от пользователя {user_id} (@{username})")
    
    try:
        # Отвечаем на callback, чтобы убрать индикатор загрузки
        await callback.answer()
        
        # Создаем фиктивное сообщение для переиспользования существующих обработчиков
        class FakeMessage:
            def __init__(self, user_id, username, first_name, text, original_message):
                self.from_user = type('obj', (object,), {
                    'id': user_id,
                    'username': username,
                    'first_name': first_name
                })()
                self.text = text
                self._original_message = original_message
                self._callback = callback
            
            async def reply(self, text, reply_markup=None):
                # Обновляем сообщение вместо отправки нового
                keyboard = get_main_keyboard(user_id) if reply_markup is None else reply_markup
                try:
                    await callback.message.edit_text(text, reply_markup=keyboard)
                except Exception as e:
                    logger.warning(f"Не удалось обновить сообщение, отправляю новое: {e}")
                    # Если не удалось обновить (например, текст не изменился), отправляем новое
                    await callback.message.answer(text, reply_markup=keyboard)
        
        fake_message = FakeMessage(user_id, username, first_name, command, callback.message)
        
        # Перенаправляем на соответствующий обработчик
        if command == "cmd_my_schedule":
            logger.info(f"Выполняю команду my_schedule для пользователя {user_id}")
            await cmd_my_schedule(fake_message)
        elif command == "cmd_full_schedule":
            logger.info(f"Выполняю команду full_schedule для пользователя {user_id}")
            await cmd_full_schedule(fake_message)
        elif command == "cmd_add_day":
            response = (
                "➕ Добавить день в расписание\n\n"
                "Используйте команду:\n"
                "/add_day [дата]\n\n"
                "Пример:\n"
                "/add_day 2024-12-20\n\n"
                "Можно указать несколько дат:\n"
                "/add_day 2024-12-20 2024-12-21"
            )
            keyboard = get_main_keyboard(user_id)
            await fake_message.reply(response, reply_markup=keyboard)
            log_command(user_id, username, first_name, "button_add_day", response[:200])
        elif command == "cmd_skip_day":
            response = (
                "➖ Пропустить день в расписании\n\n"
                "Используйте команду:\n"
                "/skip_day [дата]\n\n"
                "Пример:\n"
                "/skip_day 2024-12-20\n\n"
                "Можно указать несколько дат:\n"
                "/skip_day 2024-12-20 2024-12-21"
            )
            keyboard = get_main_keyboard(user_id)
            await fake_message.reply(response, reply_markup=keyboard)
            log_command(user_id, username, first_name, "button_skip_day", response[:200])
        elif command == "cmd_set_week_days":
            response = (
                "📝 Указать дни на следующую неделю\n\n"
                "Используйте команду:\n"
                "/set_week_days [даты]\n\n"
                "Пример с датами:\n"
                "/set_week_days 2024-12-23 2024-12-24 2024-12-26\n\n"
                "Или с названиями дней:\n"
                "/set_week_days пн вт чт"
            )
            keyboard = get_main_keyboard(user_id)
            await fake_message.reply(response, reply_markup=keyboard)
            log_command(user_id, username, first_name, "button_set_week_days", response[:200])
        elif command == "cmd_help":
            logger.info(f"Выполняю команду help для пользователя {user_id}")
            await cmd_help(fake_message)
        elif command == "cmd_admin_add_employee":
            response = (
                "👤 Добавить сотрудника\n\n"
                "Используйте команду:\n"
                "/admin_add_employee [имя] @username\n\n"
                "Или ответьте на сообщение пользователя:\n"
                "/admin_add_employee [имя]\n\n"
                "Пример:\n"
                "/admin_add_employee Иван @ivan_user"
            )
            keyboard = get_main_keyboard(user_id)
            await fake_message.reply(response, reply_markup=keyboard)
            log_command(user_id, username, first_name, "button_admin_add_employee", response[:200])
        elif command == "cmd_admin_add_admin":
            response = (
                "👑 Добавить администратора\n\n"
                "Используйте команду:\n"
                "/admin_add_admin @username\n\n"
                "Пример:\n"
                "/admin_add_admin @admin_user"
            )
            keyboard = get_main_keyboard(user_id)
            await fake_message.reply(response, reply_markup=keyboard)
            log_command(user_id, username, first_name, "button_admin_add_admin", response[:200])
        elif command == "cmd_admin_list_admins":
            logger.info(f"Выполняю команду admin_list_admins для пользователя {user_id}")
            await cmd_admin_list_admins(fake_message)
        elif command == "cmd_admin_sync_from_sheets":
            logger.info(f"Выполняю команду admin_sync_from_sheets для пользователя {user_id}")
            await cmd_admin_sync_from_sheets(fake_message)
        else:
            logger.warning(f"Неизвестная команда callback: {command}")
            await callback.answer("Неизвестная команда", show_alert=True)
        
        # Синхронизируем только если команда изменяет данные
        # Команды, которые только читают: cmd_my_schedule, cmd_full_schedule, cmd_help, cmd_admin_list_admins
        # Команды, которые изменяют: cmd_set_week_days, cmd_add_day, cmd_skip_day (через подсказки не изменяют)
        # Для команд с подсказками синхронизация не нужна, так как они только показывают инструкции
    except Exception as e:
        logger.error(f"❌ Ошибка при обработке callback {command}: {e}", exc_info=True)
        try:
            await callback.answer(f"Ошибка: {str(e)[:50]}", show_alert=True)
        except:
            pass


# Запуск бота
async def main():
    """Основная функция запуска бота"""
    # Инициализируем данные при первом запуске
    init_all()
    
    # Инициализируем PostgreSQL если доступен
    from database import init_db, test_connection
    logger.info("🔧 Начинаю инициализацию PostgreSQL...")
    use_postgresql = await init_db()
    logger.info(f"   init_db() вернул: {use_postgresql}")
    # Проверяем _pool через прямой доступ к модулю (после инициализации)
    import database as db_module
    logger.info(f"   _pool после init_db(): {db_module._pool is not None if hasattr(db_module, '_pool') else False}")
    if use_postgresql:
        await test_connection()
        logger.info(f"   _pool после test_connection(): {db_module._pool is not None if hasattr(db_module, '_pool') else False}")
        logger.info("✅ PostgreSQL инициализирован и готов к работе")
        
        # Опциональное тестирование операций PostgreSQL (включить через TEST_POSTGRESQL=true)
        if os.getenv('TEST_POSTGRESQL', 'false').lower() == 'true':
            logger.info("🧪 Тестирование операций PostgreSQL...")
            test_date_str = "2099-12-31"
            test_day_name = "Понедельник"
            test_employees = "Тест1, Тест2"
            
            try:
                from database_sync import save_schedule_to_db_sync, load_schedule_from_db_sync
                
                # Тест 1: Создание записи
                logger.info("🧪 Тест 1: Создание записи в PostgreSQL...")
                create_result = save_schedule_to_db_sync(test_date_str, test_day_name, test_employees)
                if create_result:
                    logger.info("✅ Тест 1 ПРОЙДЕН: Запись успешно создана в PostgreSQL")
                else:
                    logger.error("❌ Тест 1 ПРОВАЛЕН: Не удалось создать запись в PostgreSQL")
                
                # Тест 2: Чтение записи
                logger.info("🧪 Тест 2: Чтение записи из PostgreSQL...")
                read_result = load_schedule_from_db_sync(test_date_str)
                if read_result and test_day_name in read_result:
                    if read_result[test_day_name] == test_employees:
                        logger.info("✅ Тест 2 ПРОЙДЕН: Запись успешно прочитана из PostgreSQL, данные совпадают")
                    else:
                        logger.error(f"❌ Тест 2 ПРОВАЛЕН: Данные не совпадают. Ожидалось: '{test_employees}', получено: '{read_result[test_day_name]}'")
                else:
                    logger.error("❌ Тест 2 ПРОВАЛЕН: Не удалось прочитать запись из PostgreSQL")
                
                # Тест 3: Обновление записи
                logger.info("🧪 Тест 3: Обновление записи в PostgreSQL...")
                updated_employees = "Тест3, Тест4"
                update_result = save_schedule_to_db_sync(test_date_str, test_day_name, updated_employees)
                if update_result:
                    read_updated = load_schedule_from_db_sync(test_date_str)
                    if read_updated and read_updated.get(test_day_name) == updated_employees:
                        logger.info("✅ Тест 3 ПРОЙДЕН: Запись успешно обновлена в PostgreSQL")
                    else:
                        logger.error("❌ Тест 3 ПРОВАЛЕН: Запись не обновилась корректно")
                else:
                    logger.error("❌ Тест 3 ПРОВАЛЕН: Не удалось обновить запись в PostgreSQL")
                
                # Тест 4: Удаление записи
                logger.info("🧪 Тест 4: Удаление записи из PostgreSQL...")
                delete_result = save_schedule_to_db_sync(test_date_str, test_day_name, "")
                if delete_result:
                    read_deleted = load_schedule_from_db_sync(test_date_str)
                    if not read_deleted or test_day_name not in read_deleted or not read_deleted[test_day_name]:
                        logger.info("✅ Тест 4 ПРОЙДЕН: Запись успешно удалена из PostgreSQL")
                    else:
                        logger.warning("⚠️ Тест 4: Запись не удалена (возможно, пустая строка не удаляет запись)")
                else:
                    logger.error("❌ Тест 4 ПРОВАЛЕН: Не удалось удалить запись из PostgreSQL")
                
                logger.info("🧪 Тестирование PostgreSQL завершено")
            except Exception as e:
                logger.error(f"❌ ОШИБКА при тестировании PostgreSQL: {e}", exc_info=True)
    else:
        logger.warning("⚠️ PostgreSQL не инициализирован, проверьте логи выше для деталей")
        logger.info("⚠️ PostgreSQL недоступен, используем Google Sheets")
        use_postgresql = False
    
    # Явно загружаем данные из Google Sheets при старте бота (если PostgreSQL не используется)
    if not use_postgresql:
        logger.info("Загрузка данных из Google Sheets при старте...")
    try:
        # Перезагружаем данные сотрудников
        employee_manager.reload_employees()
        employee_manager.reload_pending_employees()
        logger.info("✅ Сотрудники загружены")
        
        # Перезагружаем администраторов
        admin_manager.reload_admins()
        logger.info("✅ Администраторы загружены")
        
        # Загружаем расписание по умолчанию (это вызовет загрузку из Google Sheets)
        schedule_manager.load_default_schedule()
        logger.info("✅ Расписание по умолчанию загружено")
        
        # Предзагружаем расписания из Google Sheets для текущей и следующей недели
        # Это гарантирует, что данные будут доступны при первом вызове команд
        now = datetime.now(timezone)
        current_week_start = schedule_manager.get_week_start(now)
        next_week_start = current_week_start + timedelta(days=7)
        
        # Предзагружаем расписания для текущей недели
        week_dates = schedule_manager.get_week_dates(current_week_start)
        for d, day_name in week_dates:
            try:
                schedule_manager.load_schedule_for_date(d, employee_manager)
            except Exception as e:
                logger.debug(f"Не удалось предзагрузить расписание для {d.strftime('%Y-%m-%d')}: {e}")
        
        # Предзагружаем расписания для следующей недели
        week_dates = schedule_manager.get_week_dates(next_week_start)
        for d, day_name in week_dates:
            try:
                schedule_manager.load_schedule_for_date(d, employee_manager)
            except Exception as e:
                logger.debug(f"Не удалось предзагрузить расписание для {d.strftime('%Y-%m-%d')}: {e}")
        
        logger.info("✅ Расписания предзагружены из Google Sheets")
        logger.info("✅ Все данные успешно загружены из Google Sheets")
    except Exception as e:
        logger.error(f"❌ Ошибка при загрузке данных из Google Sheets: {e}", exc_info=True)
        logger.warning("Продолжаем работу с локальными файлами...")
    
    # Запускаем менеджер уведомлений
    notification_manager.start()
    
    # Запускаем задачу для периодической отправки буферизованных логов
    from logger import flush_log_buffer
    asyncio.create_task(flush_log_buffer())
    logger.info("Запущена задача для отправки буферизованных логов")
    
    # Запускаем задачу для периодической отправки всех буферизованных операций в Google Sheets
    if schedule_manager.sheets_manager:
        schedule_manager.sheets_manager.start_buffer_flusher()
    if employee_manager.sheets_manager:
        employee_manager.sheets_manager.start_buffer_flusher()
    if admin_manager.sheets_manager:
        admin_manager.sheets_manager.start_buffer_flusher()
    
    # Инициализируем глобальные переменные для синхронизации
    global _sync_lock
    _sync_lock = asyncio.Lock()
    logger.info("Инициализирован lock для синхронизации")
    
    # Запускаем задачу для периодической синхронизации PostgreSQL -> Google Sheets (каждые 10 минут)
    async def sync_postgresql_to_sheets_periodically():
        """Периодическая синхронизация данных из PostgreSQL в Google Sheets"""
        from config import USE_GOOGLE_SHEETS
        if not USE_GOOGLE_SHEETS:
            return
        
        # Ждем 1 минуту после старта бота перед первой синхронизацией
        await asyncio.sleep(60)  # 1 минута
        
        while True:
            try:
                await sync_postgresql_to_sheets()
            except Exception as e:
                logger.error(f"❌ Ошибка при периодической синхронизации: {e}", exc_info=True)
            
            # Ждем 10 минут до следующей синхронизации
            await asyncio.sleep(600)  # 10 минут
    
    # Запускаем задачу синхронизации только если PostgreSQL используется
    if use_postgresql:
        asyncio.create_task(sync_postgresql_to_sheets_periodically())
        logger.info("Запущена задача для периодической синхронизации PostgreSQL -> Google Sheets (каждые 10 минут)")
    
    # Middleware для синхронизации удален - синхронизация происходит только после команд, изменяющих данные
    
    # Запускаем простой HTTP-сервер для health check (в отдельном потоке)
    health_thread = threading.Thread(target=start_health_server, daemon=True)
    health_thread.start()
    logger.info("Health check server thread started")
    
    # Удаляем вебхук и запускаем polling
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("Вебхук удален, запускаем polling...")
    except Exception as e:
        logger.warning(f"Не удалось удалить вебхук (возможно, его не было): {e}")
    
    # Запускаем polling с обработкой ошибок
    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    except Exception as e:
        logger.error(f"Критическая ошибка при запуске polling: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    # Принудительный вывод для диагностики (виден в логах контейнера)
    print("=" * 50)
    print("🚀 Запуск бота...")
    print("=" * 50)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        force=True  # Принудительная настройка логирования
    )
    
    # Дополнительный вывод в stdout для диагностики
    import sys
    sys.stdout.flush()
    
    logger.info("=" * 50)
    logger.info("🚀 Бот запущен...")
    logger.info("=" * 50)
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
        # Закрываем подключение к БД при остановке
        from database import close_db
        asyncio.run(close_db())
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}", exc_info=True)
        print(f"❌ Критическая ошибка: {e}")
        # Закрываем подключение к БД при ошибке
        from database import close_db
        try:
            asyncio.run(close_db())
        except:
            pass
        raise

