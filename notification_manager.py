"""
Управление уведомлениями и автоматическими задачами
"""
import asyncio
from datetime import datetime, timedelta
from typing import List
from aiogram import Bot
from schedule_manager import ScheduleManager
from employee_manager import EmployeeManager
from config import REMINDER_HOUR, REMINDER_MINUTE, SCHEDULE_SEND_HOUR, SCHEDULE_SEND_MINUTE, TIMEZONE, MAX_OFFICE_SEATS
import pytz


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


class NotificationManager:
    """Класс для управления уведомлениями"""
    
    def __init__(self, bot: Bot, schedule_manager: ScheduleManager, 
                 employee_manager: EmployeeManager):
        self.bot = bot
        self.schedule_manager = schedule_manager
        self.employee_manager = employee_manager
        self.timezone = pytz.timezone(TIMEZONE)
        self.running = False
    
    async def send_reminder(self):
        """Отправить напоминание всем сотрудникам о необходимости указать дни"""
        telegram_ids = self.employee_manager.get_all_telegram_ids()
        
        message = (
            "🔔 Напоминание!\n\n"
            "До воскресенья вечера укажите дни, когда вам нужно быть в офисе на следующей неделе.\n\n"
            "Используйте команду:\n"
            "/set_week_days пн вт чт\n\n"
            "Или ответьте на это сообщение списком дней (например: \"пн, вт, чт\")"
        )
        
        for telegram_id in telegram_ids:
            try:
                await self.bot.send_message(telegram_id, message)
            except Exception as e:
                print(f"Ошибка отправки напоминания {telegram_id}: {e}")
    
    async def send_weekly_schedule(self):
        """Отправить финальное расписание всем сотрудникам"""
        now = datetime.now(self.timezone)
        next_week_start = self.schedule_manager.get_week_start(now + timedelta(days=7))
        
        # Загружаем заявки и формируем расписание
        requests = self.schedule_manager.load_requests_for_week(next_week_start)
        
        if not requests:
            # Если заявок нет, используем расписание по умолчанию
            schedule = self.schedule_manager.load_default_schedule()
        else:
            schedule = self.schedule_manager.build_schedule_from_requests(
                next_week_start, requests, self.employee_manager
            )
        
        # Обрабатываем очередь для каждого дня следующей недели
        # (если есть свободные места после применения заявок)
        week_dates = self.schedule_manager.get_week_dates(next_week_start)
        for date, day_name in week_dates:
            # Проверяем, есть ли место в расписании
            employees = schedule.get(day_name, [])
            if len(employees) < MAX_OFFICE_SEATS:
                # Обрабатываем очередь для этого дня
                added_from_queue = self.schedule_manager.process_queue_for_date(date, self.employee_manager)
                if added_from_queue:
                    # Обновляем расписание
                    schedule[day_name] = self.schedule_manager.load_schedule_for_date(date, self.employee_manager).get(day_name, [])
                    # Уведомляем добавленного из очереди
                    try:
                        await self.bot.send_message(
                            added_from_queue['telegram_id'],
                            f"✅ Место освободилось!\n\n"
                            f"📅 {day_to_short(day_name)} ({date.strftime('%d.%m.%Y')})\n"
                            f"Вы автоматически добавлены в расписание на следующую неделю."
                        )
                    except Exception as e:
                        print(f"Ошибка отправки уведомления {added_from_queue['telegram_id']}: {e}")
        
        # Сохраняем финальное расписание (с учетом очереди)
        self.schedule_manager.save_schedule_for_week(next_week_start, schedule)
        
        # Получаем информацию о свободных местах
        available_slots = self.schedule_manager.get_available_slots(schedule)
        
        # Загружаем расписание по умолчанию для сравнения
        default_schedule = self.schedule_manager.load_default_schedule()
        
        # Отправляем каждому сотруднику его расписание
        all_employees = self.employee_manager.get_all_employees()
        
        # Получаем даты недели для определения расписания сотрудника
        week_dates = self.schedule_manager.get_week_dates(next_week_start)
        
        for employee_name, telegram_id in all_employees.items():
            # Получаем расписание сотрудника из уже построенного расписания
            employee_schedule = {}
            formatted_name = self.employee_manager.format_employee_name(employee_name)
            
            for date, day_name in week_dates:
                employees = schedule.get(day_name, [])
                employee_schedule[day_name] = formatted_name in employees
            
            # Определяем, какие дни были запрошены дополнительно
            # (не были в расписании по умолчанию)
            additional_requests = []
            for req in requests:
                if req['employee_name'] == employee_name:
                    for day in req['days_requested']:
                        # Проверяем, был ли сотрудник в этом дне в расписании по умолчанию
                        was_in_default = False
                        if day in default_schedule:
                            for emp in default_schedule[day]:
                                plain_name = self.schedule_manager.get_plain_name_from_formatted(emp)
                                if plain_name == employee_name:
                                    was_in_default = True
                                    break
                        
                        # Если не был в расписании по умолчанию, это дополнительный запрос
                        if not was_in_default:
                            # Проверяем, добавился ли в финальное расписание
                            got_place = employee_schedule.get(day, False)
                            additional_requests.append({
                                'day': day,
                                'got_place': got_place
                            })
                    break
            
            # Формируем сообщение
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
            
            # Информация о дополнительно запрошенных днях
            if additional_requests:
                message += f"\n📝 Дополнительно запрошенные дни:\n"
                for req_info in additional_requests:
                    day_short = day_to_short(req_info['day'])
                    if req_info['got_place']:
                        message += f"✅ {day_short} - место найдено\n"
                    else:
                        message += f"❌ {day_short} - свободного места не нашлось\n"
            
            # Информация о свободных местах в дни, которых нет в расписании
            free_slots_info = []
            for day, slots in available_slots.items():
                if day not in office_days and slots > 0:
                    free_slots_info.append(f"{day_to_short(day)}: {slots} место(а)")
            
            if free_slots_info:
                message += f"\n💡 Свободные места:\n"
                message += "\n".join(free_slots_info)
            
            try:
                await self.bot.send_message(telegram_id, message)
            except Exception as e:
                print(f"Ошибка отправки расписания {telegram_id}: {e}")
        
        # Очищаем заявки после отправки
        self.schedule_manager.clear_requests_for_week(next_week_start)
    
    async def merge_duplicates_daily(self):
        """Ежедневное схлопывание дубликатов сотрудников"""
        try:
            self.employee_manager.merge_duplicates()
            print(f"[{datetime.now(self.timezone).strftime('%Y-%m-%d %H:%M:%S')}] Выполнено схлопывание дубликатов сотрудников")
        except Exception as e:
            print(f"Ошибка при схлопывании дубликатов: {e}")
    
    async def check_and_send_reminders(self):
        """Проверять и отправлять напоминания в нужное время"""
        last_merge_date = None
        while self.running:
            try:
                now = datetime.now(self.timezone)
                
                # Ежедневное схлопывание дубликатов в 03:00
                if now.hour == 3 and now.minute == 0:
                    current_date = now.date()
                    if last_merge_date != current_date:
                        await self.merge_duplicates_daily()
                        last_merge_date = current_date
                        # Ждем минуту, чтобы не выполнить повторно
                        await asyncio.sleep(60)
                
                # Пятница 18:00 - напоминание
                if now.weekday() == 4 and now.hour == REMINDER_HOUR and now.minute == REMINDER_MINUTE:
                    await self.send_reminder()
                    # Ждем минуту, чтобы не отправить повторно
                    await asyncio.sleep(60)
                
                # Воскресенье 20:00 - рассылка расписания
                if now.weekday() == 6 and now.hour == SCHEDULE_SEND_HOUR and now.minute == SCHEDULE_SEND_MINUTE:
                    await self.send_weekly_schedule()
                    # Ждем минуту, чтобы не отправить повторно
                    await asyncio.sleep(60)
                
                # Проверяем каждую минуту
                await asyncio.sleep(60)
            except Exception as e:
                print(f"Ошибка в check_and_send_reminders: {e}")
                await asyncio.sleep(60)
    
    def start(self):
        """Запустить менеджер уведомлений"""
        self.running = True
        asyncio.create_task(self.check_and_send_reminders())
    
    def stop(self):
        """Остановить менеджер уведомлений"""
        self.running = False
    
    async def notify_available_slot(self, date: datetime, day_name: str, free_slots: int):
        """Уведомить всех сотрудников о свободном месте в текущей неделе"""
        if free_slots <= 0:
            return
        
        date_str = date.strftime('%d.%m.%Y')
        day_short = day_to_short(day_name)
        
        message = (
            f"💡 Свободное место в офисе!\n\n"
            f"📅 {day_short} ({date_str})\n"
            f"🆓 Доступно мест: {free_slots}\n\n"
            f"Используйте команду /add_day {date.strftime('%Y-%m-%d')} чтобы занять место"
        )
        
        # Получаем всех сотрудников
        all_employees = self.employee_manager.get_all_employees()
        
        # Загружаем расписание на эту дату
        schedule = self.schedule_manager.load_schedule_for_date(date, self.employee_manager)
        employees_in_office = schedule.get(day_name, [])
        
        # Отправляем уведомление всем, кто не в офисе в этот день
        for employee_name, telegram_id in all_employees.items():
            formatted_name = self.employee_manager.format_employee_name(employee_name)
            if formatted_name not in employees_in_office:
                try:
                    await self.bot.send_message(telegram_id, message)
                except Exception as e:
                    print(f"Ошибка отправки уведомления {telegram_id}: {e}")

