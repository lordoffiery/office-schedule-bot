"""
Управление сотрудниками
"""
import os
from typing import Dict, Optional, List, Tuple
from config import EMPLOYEES_FILE, DATA_DIR, PENDING_EMPLOYEES_FILE


class EmployeeManager:
    """Класс для управления списком сотрудников"""
    
    def __init__(self):
        # Формат: telegram_id -> (имя_вручную, имя_из_телеги, никнейм)
        self.employees: Dict[int, Tuple[str, str, Optional[str]]] = {}
        # Для обратного поиска: имя_вручную -> telegram_id
        self.name_to_id: Dict[str, int] = {}
        # Отложенные записи: username -> manual_name (для случаев, когда админ добавляет по username до /start)
        self.pending_employees: Dict[str, str] = {}
        self._load_employees()
        self._load_pending_employees()
    
    def _load_employees(self):
        """Загрузить список сотрудников из файла"""
        if not os.path.exists(EMPLOYEES_FILE):
            os.makedirs(DATA_DIR, exist_ok=True)
            return
        
        try:
            with open(EMPLOYEES_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line or ':' not in line:
                        continue
                    
                    parts = line.split(':')
                    # Поддержка старого формата (имя:telegram_id) и нового (имя:имя_телеги:id:никнейм)
                    if len(parts) == 2:
                        # Старый формат: имя:telegram_id
                        manual_name = parts[0].strip()
                        telegram_id = int(parts[1].strip())
                        telegram_name = manual_name
                        username = None
                    elif len(parts) >= 3:
                        # Новый формат: имя_вручную:имя_телеги:telegram_id:никнейм
                        manual_name = parts[0].strip()
                        telegram_name = parts[1].strip() if len(parts) > 1 and parts[1].strip() else manual_name
                        telegram_id = int(parts[2].strip())
                        username = parts[3].strip() if len(parts) > 3 and parts[3].strip() else None
                    else:
                        continue
                    
                    # Если уже есть запись с таким ID, пропускаем (будет схлопнуто позже)
                    if telegram_id not in self.employees:
                        self.employees[telegram_id] = (manual_name, telegram_name, username)
                        self.name_to_id[manual_name] = telegram_id
        except Exception as e:
            print(f"Ошибка загрузки сотрудников: {e}")
    
    def _save_employees(self):
        """Сохранить список сотрудников в файл"""
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(EMPLOYEES_FILE, 'w', encoding='utf-8') as f:
            for telegram_id in sorted(self.employees.keys()):
                manual_name, telegram_name, username = self.employees[telegram_id]
                username_str = username if username else ""
                f.write(f"{manual_name}:{telegram_name}:{telegram_id}:{username_str}\n")
    
    def add_employee(self, name: str, telegram_id: int, telegram_name: Optional[str] = None, username: Optional[str] = None) -> bool:
        """Добавить сотрудника"""
        # Если уже есть запись с таким ID, сохраняем существующие данные
        old_username = None
        old_telegram_name = None
        if telegram_id in self.employees:
            old_manual_name, old_telegram_name, old_username = self.employees[telegram_id]
            # Удаляем старое имя из индекса
            if old_manual_name in self.name_to_id:
                del self.name_to_id[old_manual_name]
        
        # Если имя уже используется другим ID, удаляем старую связь
        if name in self.name_to_id and self.name_to_id[name] != telegram_id:
            old_id = self.name_to_id[name]
            if old_id in self.employees:
                del self.employees[old_id]
        
        # Сохраняем существующие данные, если новые не указаны
        telegram_name = telegram_name or old_telegram_name or name
        username = username if username is not None else old_username
        
        self.employees[telegram_id] = (name, telegram_name, username)
        self.name_to_id[name] = telegram_id
        self._save_employees()
        return True
    
    def get_employee_name(self, telegram_id: int) -> Optional[str]:
        """Получить имя сотрудника по Telegram ID (возвращает имя_вручную)"""
        if telegram_id in self.employees:
            return self.employees[telegram_id][0]
        return None
    
    def get_employee_id(self, name: str) -> Optional[int]:
        """Получить Telegram ID сотрудника по имени"""
        return self.name_to_id.get(name)
    
    def get_telegram_id_by_username(self, username: str) -> Optional[int]:
        """Получить Telegram ID по username (никнейму в Telegram)"""
        username_clean = username.lower().lstrip('@')
        for telegram_id, (_, _, user_username) in self.employees.items():
            if user_username and user_username.lower() == username_clean:
                return telegram_id
        return None
    
    def _load_pending_employees(self):
        """Загрузить отложенные записи сотрудников (username -> manual_name)"""
        if not os.path.exists(PENDING_EMPLOYEES_FILE):
            return
        
        try:
            with open(PENDING_EMPLOYEES_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line or ':' not in line:
                        continue
                    parts = line.split(':', 1)
                    if len(parts) == 2:
                        username = parts[0].strip().lower()
                        manual_name = parts[1].strip()
                        self.pending_employees[username] = manual_name
        except Exception as e:
            print(f"Ошибка загрузки отложенных записей: {e}")
    
    def _save_pending_employees(self):
        """Сохранить отложенные записи сотрудников"""
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(PENDING_EMPLOYEES_FILE, 'w', encoding='utf-8') as f:
            for username, manual_name in sorted(self.pending_employees.items()):
                f.write(f"{username}:{manual_name}\n")
    
    def add_pending_employee(self, username: str, manual_name: str):
        """Добавить отложенную запись сотрудника (когда админ добавляет по username до /start)"""
        username_lower = username.lower().lstrip('@')
        self.pending_employees[username_lower] = manual_name
        self._save_pending_employees()
    
    def get_pending_employee(self, username: str) -> Optional[str]:
        """Получить имя вручную из отложенной записи по username"""
        username_lower = username.lower().lstrip('@')
        return self.pending_employees.get(username_lower)
    
    def remove_pending_employee(self, username: str):
        """Удалить отложенную запись после успешной регистрации"""
        username_lower = username.lower().lstrip('@')
        if username_lower in self.pending_employees:
            del self.pending_employees[username_lower]
            self._save_pending_employees()
    
    def register_user(self, telegram_id: int, telegram_name: str, username: Optional[str] = None) -> bool:
        """Зарегистрировать пользователя (если его еще нет)"""
        if telegram_id in self.employees:
            # Обновляем данные, если они изменились
            manual_name, old_telegram_name, old_username = self.employees[telegram_id]
            # Обновляем имя из Telegram и username, но сохраняем имя вручную
            updated = False
            if telegram_name != old_telegram_name or (username and username != old_username):
                self.employees[telegram_id] = (manual_name, telegram_name, username or old_username)
                self._save_employees()
                updated = True
            return False  # Уже зарегистрирован
        
        # Проверяем, есть ли отложенная запись для этого username
        manual_name = telegram_name  # По умолчанию используем имя из Telegram
        if username:
            pending_name = self.get_pending_employee(username)
            if pending_name:
                manual_name = pending_name
                # Удаляем отложенную запись, так как пользователь зарегистрирован
                self.remove_pending_employee(username)
        
        # Используем имя вручную (из отложенной записи или из Telegram)
        self.employees[telegram_id] = (manual_name, telegram_name, username)
        self.name_to_id[manual_name] = telegram_id
        self._save_employees()
        return True
    
    def get_all_employees(self) -> Dict[str, int]:
        """Получить всех сотрудников (имя -> telegram_id)"""
        return self.name_to_id.copy()
    
    def get_all_telegram_ids(self) -> List[int]:
        """Получить все Telegram ID"""
        return list(self.employees.keys())
    
    def is_registered(self, telegram_id: int) -> bool:
        """Проверить, зарегистрирован ли пользователь"""
        return telegram_id in self.employees
    
    def get_employee_data(self, telegram_id: int) -> Optional[Tuple[str, str, Optional[str]]]:
        """Получить данные сотрудника по Telegram ID (имя_вручную, имя_телеги, никнейм)"""
        return self.employees.get(telegram_id)
    
    def format_employee_name(self, employee_name: str) -> str:
        """Форматировать имя сотрудника для отображения: имя(@никнейм)"""
        telegram_id = self.name_to_id.get(employee_name)
        if telegram_id and telegram_id in self.employees:
            _, _, username = self.employees[telegram_id]
            if username:
                return f"{employee_name}(@{username})"
        return employee_name
    
    def format_employee_name_by_id(self, telegram_id: int) -> str:
        """Форматировать имя сотрудника по ID для отображения: имя(@никнейм)"""
        if telegram_id in self.employees:
            manual_name, _, username = self.employees[telegram_id]
            if username:
                return f"{manual_name}(@{username})"
            return manual_name
        return str(telegram_id)
    
    def merge_duplicates(self):
        """Схлопнуть дубликаты по telegram_id (оставить последнюю запись для каждого ID)"""
        # Загружаем все записи из файла (включая дубликаты)
        all_entries = {}
        if os.path.exists(EMPLOYEES_FILE):
            try:
                with open(EMPLOYEES_FILE, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if not line or ':' not in line:
                            continue
                        
                        parts = line.split(':')
                        # Поддержка старого и нового формата
                        if len(parts) == 2:
                            # Старый формат: имя:telegram_id
                            manual_name = parts[0].strip()
                            telegram_id = int(parts[1].strip())
                            telegram_name = manual_name
                            username = None
                        elif len(parts) >= 3:
                            # Новый формат: имя_вручную:имя_телеги:telegram_id:никнейм
                            manual_name = parts[0].strip()
                            telegram_name = parts[1].strip() if len(parts) > 1 and parts[1].strip() else manual_name
                            telegram_id = int(parts[2].strip())
                            username = parts[3].strip() if len(parts) > 3 and parts[3].strip() else None
                        else:
                            continue
                        
                        # Оставляем последнюю запись для каждого ID (схлопывание)
                        all_entries[telegram_id] = (manual_name, telegram_name, username)
            except Exception as e:
                print(f"Ошибка при схлопывании дубликатов: {e}")
                return
        
        # Обновляем внутренние структуры
        self.employees = all_entries
        self.name_to_id = {}
        for telegram_id, (manual_name, _, _) in self.employees.items():
            self.name_to_id[manual_name] = telegram_id
        
        # Сохраняем схлопнутые данные
        self._save_employees()
