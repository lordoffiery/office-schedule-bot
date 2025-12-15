"""
Модуль для инициализации данных при первом запуске
"""
import os
from config import (
    DATA_DIR, SCHEDULES_DIR, REQUESTS_DIR, QUEUE_DIR,
    EMPLOYEES_FILE, ADMINS_FILE, DEFAULT_SCHEDULE_FILE,
    PENDING_EMPLOYEES_FILE, ADMIN_IDS, DEFAULT_SCHEDULE
)


def init_data_directories():
    """Создать все необходимые директории"""
    directories = [DATA_DIR, SCHEDULES_DIR, REQUESTS_DIR, QUEUE_DIR]
    for directory in directories:
        os.makedirs(directory, exist_ok=True)


def init_default_schedule():
    """Инициализировать файл с расписанием по умолчанию, если его нет"""
    if not os.path.exists(DEFAULT_SCHEDULE_FILE):
        with open(DEFAULT_SCHEDULE_FILE, 'w', encoding='utf-8') as f:
            for day, employees in DEFAULT_SCHEDULE.items():
                f.write(f"{day}: {', '.join(employees)}\n")
        print(f"✅ Создан файл {DEFAULT_SCHEDULE_FILE}")


def init_admins_file():
    """Инициализировать файл с администраторами, если его нет"""
    if not os.path.exists(ADMINS_FILE):
        with open(ADMINS_FILE, 'w', encoding='utf-8') as f:
            for admin_id in ADMIN_IDS:
                f.write(f"{admin_id}\n")
        print(f"✅ Создан файл {ADMINS_FILE} с администраторами: {', '.join(map(str, ADMIN_IDS))}")


def init_employees_file():
    """Инициализировать файл с сотрудниками, если его нет"""
    if not os.path.exists(EMPLOYEES_FILE):
        # Создаем пустой файл
        with open(EMPLOYEES_FILE, 'w', encoding='utf-8') as f:
            pass
        print(f"✅ Создан пустой файл {EMPLOYEES_FILE}")


def init_pending_employees_file():
    """Инициализировать файл с отложенными сотрудниками, если его нет"""
    if not os.path.exists(PENDING_EMPLOYEES_FILE):
        with open(PENDING_EMPLOYEES_FILE, 'w', encoding='utf-8') as f:
            pass
        print(f"✅ Создан пустой файл {PENDING_EMPLOYEES_FILE}")


def init_all():
    """Инициализировать все необходимые файлы и директории"""
    print("🔧 Инициализация данных...")
    init_data_directories()
    init_default_schedule()
    init_admins_file()
    init_employees_file()
    init_pending_employees_file()
    print("✅ Инициализация данных завершена")

