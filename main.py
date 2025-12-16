"""
Основной файл Telegram-бота для управления расписанием сотрудников
"""
import asyncio
import os
import logging
from datetime import datetime, timedelta
from typing import Optional
from aiogram import Bot, Dispatcher
from aiogram.types import Message
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage

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
    
    await message.reply(response)
    log_command(user_id, username, user_name, "/start", response)


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
            "   Дни: Понедельник, Вторник, Среда, Четверг, Пятница"
        )
    
    await message.reply(help_text)
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
            # Проверяем, есть ли сотрудник в списке (может быть отформатированным)
            employee_in_schedule = False
            for emp in default_schedule[day]:
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
        # Используем сохраненные расписания
        schedule = {}
        for date, day_name in week_dates:
            day_schedule = schedule_manager.load_schedule_for_date(date, employee_manager)
            schedule[day_name] = day_schedule.get(day_name, [])
    else:
        # Загружаем заявки на неделю и строим расписание с учетом заявок
        requests = schedule_manager.load_requests_for_week(current_week_start)
        schedule = schedule_manager.build_schedule_from_requests(current_week_start, requests, employee_manager)
    
    # Получаем расписание сотрудника из построенного расписания
    employee_schedule = {}
    formatted_name = employee_manager.format_employee_name(employee_name)
    
    for date, day_name in week_dates:
        employees = schedule.get(day_name, [])
        employee_schedule[day_name] = formatted_name in employees
    
    message_text = format_schedule_message(employee_schedule, current_week_start)
    await message.reply(message_text)
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
        # Используем сохраненные расписания
        schedule = {}
        for d, day_name in week_dates:
            day_schedule = schedule_manager.load_schedule_for_date(d, employee_manager)
            schedule[day_name] = day_schedule.get(day_name, [])
    else:
        # Загружаем заявки на неделю и строим расписание с учетом заявок
        requests = schedule_manager.load_requests_for_week(week_start)
        schedule = schedule_manager.build_schedule_from_requests(week_start, requests, employee_manager)
    
    message_text = f"📅 Расписание на {date.strftime('%d.%m.%Y')}:\n\n"
    for day, employees in schedule.items():
        # Имена уже отформатированы
        message_text += f"{day}: {', '.join(employees)}\n"
    
    await message.reply(message_text)
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
            # Сохраняем отложенную запись для использования при /start
            employee_manager.add_pending_employee(username, name)
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
    
    # Обновляем расписание для указанного дня
    default_schedule[day_name] = employees
    
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
                    # Проверяем, есть ли сотрудник в списке (может быть отформатированным)
                    employee_in_schedule = False
                    for emp in default_schedule[day]:
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


# Запуск бота
async def main():
    """Основная функция запуска бота"""
    # Инициализируем данные при первом запуске
    init_all()
    
    # Запускаем менеджер уведомлений
    notification_manager.start()
    
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
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logger.info("Бот запущен...")
    asyncio.run(main())

