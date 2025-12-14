"""
Управление уведомлениями и автоматическими задачами
"""
import asyncio
from datetime import datetime, timedelta
from typing import List
from aiogram import Bot
from schedule_manager import ScheduleManager
from employee_manager import EmployeeManager
from config import REMINDER_HOUR, REMINDER_MINUTE, SCHEDULE_SEND_HOUR, SCHEDULE_SEND_MINUTE, TIMEZONE
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
        
        # Сохраняем расписание
        self.schedule_manager.save_schedule_for_week(next_week_start, schedule)
        
        # Получаем информацию о свободных местах
        available_slots = self.schedule_manager.get_available_slots(schedule)
        
        # Отправляем каждому сотруднику его расписание
        all_employees = self.employee_manager.get_all_employees()
        
        for employee_name, telegram_id in all_employees.items():
            employee_schedule = self.schedule_manager.get_employee_schedule(
                next_week_start, employee_name
            )
            
            # Формируем сообщение
            week_dates = self.schedule_manager.get_week_dates(next_week_start)
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
    
    async def check_and_send_reminders(self):
        """Проверять и отправлять напоминания в нужное время"""
        while self.running:
            try:
                now = datetime.now(self.timezone)
                
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

