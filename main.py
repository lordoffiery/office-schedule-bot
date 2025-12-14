"""
Основной файл Telegram-бота для управления расписанием сотрудников
"""
import asyncio
from datetime import datetime, timedelta
from typing import Optional
from aiogram import Bot, Dispatcher
from aiogram.types import Message
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

from config import API_TOKEN, ADMIN_IDS, WEEKDAYS_RU, TIMEZONE
from employee_manager import EmployeeManager
from schedule_manager import ScheduleManager
from notification_manager import NotificationManager
from admin_manager import AdminManager
import pytz


# Инициализация
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Менеджеры
admin_manager = AdminManager()
employee_manager = EmployeeManager()
schedule_manager = ScheduleManager()
notification_manager = NotificationManager(bot, schedule_manager, employee_manager)

timezone = pytz.timezone(TIMEZONE)


# FSM состояния
class ScheduleStates(StatesGroup):
    waiting_for_week_days = State()


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
    
    # Регистрируем пользователя, если его еще нет
    if not employee_manager.is_registered(user_id):
        employee_manager.register_user(user_id, user_name)
        await message.reply(
            f"Привет, {user_name}! Я бот для управления расписанием сотрудников.\n\n"
            "Используйте /help для списка команд."
        )
    else:
        await message.reply("Вы уже зарегистрированы! Используйте /help для списка команд.")


@dp.message(Command("help"))
async def cmd_help(message: Message):
    """Команда /help"""
    help_text = (
        "📋 Доступные команды:\n\n"
        "/set_week_days [дни] - Указать дни на следующую неделю\n"
        "   Пример: /set_week_days пн вт чт\n\n"
        "/my_schedule - Показать свое расписание на следующую неделю\n\n"
        "/skip_day [дата] - Пропустить день\n"
        "   Пример: /skip_day 2024-12-20\n\n"
        "/add_day [дата] - Запросить дополнительный день\n"
        "   Пример: /add_day 2024-12-20\n\n"
        "/full_schedule [дата] - Полное расписание на дату (только админы)\n\n"
        "/admin_add_employee [имя] @username - Добавить сотрудника (только админы)\n\n"
        "/admin_add_admin @username - Добавить администратора (только админы)\n\n"
        "/admin_list_admins - Список администраторов (только админы)"
    )
    await message.reply(help_text)


@dp.message(Command("set_week_days"))
async def cmd_set_week_days(message: Message, state: FSMContext):
    """Команда для установки дней на следующую неделю"""
    user_id = message.from_user.id
    
    if not employee_manager.is_registered(user_id):
        await message.reply("Вы не зарегистрированы. Используйте /start")
        return
    
    employee_name = employee_manager.get_employee_name(user_id)
    if not employee_name:
        await message.reply("Ошибка: не найдено ваше имя в системе")
        return
    
    # Парсим дни из команды
    command_parts = message.text.split(maxsplit=1)
    if len(command_parts) > 1:
        days_text = command_parts[1]
        days = parse_weekdays(days_text)
        
        if not days:
            await message.reply(
                "Не удалось распознать дни. Используйте формат:\n"
                "/set_week_days пн вт чт\n"
                "или: /set_week_days понедельник вторник четверг"
            )
            return
        
        # Получаем начало следующей недели
        now = datetime.now(timezone)
        next_week_start = schedule_manager.get_week_start(now + timedelta(days=7))
        
        # Определяем, какие дни нужно пропустить (если есть в расписании по умолчанию)
        default_schedule = schedule_manager.load_default_schedule()
        days_to_skip = []
        days_to_request = []
        
        week_days = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница']
        for day in week_days:
            if day in default_schedule and employee_name in default_schedule[day]:
                # Есть в расписании по умолчанию
                if day not in days:
                    days_to_skip.append(day)
                else:
                    days_to_request.append(day)
            else:
                # Нет в расписании по умолчанию
                if day in days:
                    days_to_request.append(day)
        
        # Сохраняем заявку
        schedule_manager.save_request(
            employee_name, user_id, next_week_start,
            days_to_request, days_to_skip
        )
        
        await message.reply(
            f"✅ Ваши дни на следующую неделю сохранены:\n"
            f"В офисе: {', '.join([day_to_short(d) for d in days])}\n\n"
            f"Финальное расписание будет отправлено в воскресенье вечером."
        )
    else:
        await message.reply(
            "Укажите дни недели. Например:\n"
            "/set_week_days пн вт чт"
        )


@dp.message(Command("my_schedule"))
async def cmd_my_schedule(message: Message):
    """Показать расписание сотрудника"""
    user_id = message.from_user.id
    
    if not employee_manager.is_registered(user_id):
        await message.reply("Вы не зарегистрированы. Используйте /start")
        return
    
    employee_name = employee_manager.get_employee_name(user_id)
    if not employee_name:
        await message.reply("Ошибка: не найдено ваше имя в системе")
        return
    
    # Получаем начало следующей недели
    now = datetime.now(timezone)
    next_week_start = schedule_manager.get_week_start(now + timedelta(days=7))
    
    # Загружаем расписание
    employee_schedule = schedule_manager.get_employee_schedule(next_week_start, employee_name)
    
    message_text = format_schedule_message(employee_schedule, next_week_start)
    await message.reply(message_text)


@dp.message(Command("full_schedule"))
async def cmd_full_schedule(message: Message):
    """Показать полное расписание на дату (только для админов)"""
    user_id = message.from_user.id
    
    if not admin_manager.is_admin(user_id):
        await message.reply("Эта команда доступна только администраторам")
        return
    
    # Парсим дату из команды
    command_parts = message.text.split()
    if len(command_parts) > 1:
        try:
            date = datetime.strptime(command_parts[1], "%Y-%m-%d")
            date = timezone.localize(date)
        except:
            await message.reply("Неверный формат даты. Используйте: /full_schedule 2024-12-20")
            return
    else:
        date = datetime.now(timezone)
    
    schedule = schedule_manager.load_schedule_for_date(date)
    
    message_text = f"📅 Расписание на {date.strftime('%d.%m.%Y')}:\n\n"
    for day, employees in schedule.items():
        message_text += f"{day}: {', '.join(employees)}\n"
    
    await message.reply(message_text)


@dp.message(Command("admin_add_employee"))
async def cmd_admin_add_employee(message: Message):
    """Добавить сотрудника (только для админов)"""
    user_id = message.from_user.id
    
    if not admin_manager.is_admin(user_id):
        await message.reply("Эта команда доступна только администраторам")
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
    
    # Парсим команду
    command_parts = message.text.split(maxsplit=2)
    
    if len(command_parts) < 2:
        await message.reply(
            "Используйте один из форматов:\n\n"
            "1. Ответьте на сообщение пользователя:\n"
            "   /admin_add_employee [имя]\n\n"
            "2. Укажите username:\n"
            "   /admin_add_employee [имя] @username\n\n"
            "3. Укажите telegram_id (если знаете):\n"
            "   /admin_add_employee [имя] [telegram_id]"
        )
        return
    
    name = command_parts[1]
    
    # Если не нашли через reply или entities, пытаемся парсить из текста
    if not telegram_id:
        if len(command_parts) >= 3:
            username_or_id = command_parts[2].lstrip('@')
            # Пытаемся понять, это ID или username
            try:
                telegram_id = int(username_or_id)
            except ValueError:
                # Это username - просим пользователя написать боту или ответить на его сообщение
                username = username_or_id
                await message.reply(
                    f"Для добавления сотрудника {name} (@{username}) используйте один из способов:\n\n"
                    f"1. Ответьте на любое сообщение от @{username} командой:\n"
                    f"   /admin_add_employee {name}\n\n"
                    f"2. Попросите @{username} написать боту /start, затем используйте команду снова"
                )
                return
        else:
            await message.reply(
                "Укажите username или ответьте на сообщение пользователя:\n"
                "/admin_add_employee [имя] @username"
            )
            return
    
    # Если у нас есть ID, добавляем сотрудника
    if telegram_id:
        if employee_manager.add_employee(name, telegram_id):
            username_display = f" (@{username})" if username else ""
            await message.reply(
                f"✅ Сотрудник {name}{username_display} добавлен\n"
                f"Telegram ID: {telegram_id}"
            )
        else:
            existing_id = employee_manager.get_employee_id(name)
            await message.reply(
                f"❌ Сотрудник {name} уже существует\n"
                f"Текущий Telegram ID: {existing_id}"
            )
    else:
        await message.reply("Не удалось определить Telegram ID пользователя")


@dp.message(Command("admin_add_admin"))
async def cmd_admin_add_admin(message: Message):
    """Добавить администратора (только для админов)"""
    user_id = message.from_user.id
    
    if not admin_manager.is_admin(user_id):
        await message.reply("Эта команда доступна только администраторам")
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
                username = username_or_id
                await message.reply(
                    f"Для добавления администратора @{username} используйте один из способов:\n\n"
                    f"1. Ответьте на любое сообщение от @{username} командой:\n"
                    f"   /admin_add_admin\n\n"
                    f"2. Попросите @{username} написать боту /start, затем используйте команду снова"
                )
                return
        else:
            await message.reply(
                "Используйте один из форматов:\n\n"
                "1. Ответьте на сообщение пользователя:\n"
                "   /admin_add_admin\n\n"
                "2. Укажите username:\n"
                "   /admin_add_admin @username\n\n"
                "3. Укажите telegram_id:\n"
                "   /admin_add_admin [telegram_id]"
            )
            return
    
    # Если у нас есть ID, добавляем администратора
    if telegram_id:
        if admin_manager.add_admin(telegram_id):
            username_display = f" (@{username})" if username else ""
            await message.reply(
                f"✅ Администратор{username_display} добавлен\n"
                f"Telegram ID: {telegram_id}"
            )
        else:
            await message.reply(
                f"❌ Пользователь уже является администратором\n"
                f"Telegram ID: {telegram_id}"
            )
    else:
        await message.reply("Не удалось определить Telegram ID пользователя")


@dp.message(Command("admin_list_admins"))
async def cmd_admin_list_admins(message: Message):
    """Показать список администраторов (только для админов)"""
    user_id = message.from_user.id
    
    if not admin_manager.is_admin(user_id):
        await message.reply("Эта команда доступна только администраторам")
        return
    
    admins = admin_manager.get_all_admins()
    
    if not admins:
        await message.reply("Список администраторов пуст")
        return
    
    message_text = "👑 Список администраторов:\n\n"
    for admin_id in admins:
        message_text += f"• {admin_id}\n"
    
    await message.reply(message_text)


# Обработка текстовых сообщений (для ответов на напоминания)
@dp.message()
async def handle_text_message(message: Message):
    """Обработка текстовых сообщений (ответы на напоминания)"""
    user_id = message.from_user.id
    
    if not employee_manager.is_registered(user_id):
        return
    
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
                if day in default_schedule and employee_name in default_schedule[day]:
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
            
            await message.reply(
                f"✅ Ваши дни на следующую неделю сохранены:\n"
                f"В офисе: {', '.join([day_to_short(d) for d in days])}\n\n"
                f"Финальное расписание будет отправлено в воскресенье вечером."
            )


# Запуск бота
async def main():
    """Основная функция запуска бота"""
    # Запускаем менеджер уведомлений
    notification_manager.start()
    
    # Удаляем вебхук и запускаем polling
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    print("Бот запущен...")
    asyncio.run(main())

