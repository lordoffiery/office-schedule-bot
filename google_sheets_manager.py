"""
Модуль для работы с Google Sheets в качестве хранилища данных
"""
import os
import json
import logging
import time
import asyncio
from typing import Dict, List, Optional, Any, Tuple
from collections import deque
from dataclasses import dataclass
from enum import Enum
import gspread
from google.oauth2.service_account import Credentials

# Настройка логирования
logger = logging.getLogger(__name__)

# Приоритеты операций
PRIORITY_HIGH = 1  # Критичные данные (employees, schedules, requests, queue)
PRIORITY_LOW = 0   # Логи

# Лимиты Google Sheets API: 100 запросов в 100 секунд
API_RATE_LIMIT = 100
API_TIME_WINDOW = 100  # секунд

# Интервал повторной попытки отправки буферизованных данных
BUFFER_RETRY_INTERVAL = 60  # секунд


class OperationType(Enum):
    """Типы операций для буферизации"""
    APPEND_ROW = "append_row"
    WRITE_ROWS = "write_rows"
    UPDATE_ROW = "update_row"
    DELETE_ROW = "delete_row"
    SET_CELL = "set_cell"


@dataclass
class BufferedOperation:
    """Операция, которая не удалась и была добавлена в буфер"""
    operation_type: OperationType
    worksheet_name: str
    data: Any  # Может быть row, rows, cell, value и т.д.
    priority: int
    timestamp: float


class GoogleSheetsManager:
    """Класс для управления данными через Google Sheets"""
    
    def __init__(self):
        self.client = None
        self.spreadsheet = None
        # Отслеживание запросов для контроля лимитов API
        self.request_times = deque()  # Временные метки последних запросов
        # Буфер для несохраненных операций (из-за ошибок API)
        self.operation_buffer: deque = deque(maxlen=5000)  # Максимум 5000 операций
        self._buffer_flusher_task = None
        self._init_client()
    
    def _init_client(self):
        """Инициализировать клиент Google Sheets"""
        # Получаем credentials из переменной окружения или файла
        credentials_json = os.getenv('GOOGLE_SHEETS_CREDENTIALS')
        spreadsheet_id = os.getenv('GOOGLE_SHEETS_ID')
        credentials_file = os.getenv('GOOGLE_CREDENTIALS_FILE', 'google_credentials.json')
        
        # Если нет переменных окружения, пробуем использовать файл
        if not credentials_json:
            if os.path.exists(credentials_file):
                try:
                    with open(credentials_file, 'r') as f:
                        credentials_dict = json.load(f)
                    credentials_json = json.dumps(credentials_dict)
                except Exception as e:
                    logger.warning(f"Не удалось прочитать файл credentials: {e}")
        
        if not credentials_json or not spreadsheet_id:
            logger.warning("Google Sheets не настроен. Используются локальные файлы.")
            return None
        
        try:
            # Парсим JSON credentials
            if isinstance(credentials_json, str):
                credentials_dict = json.loads(credentials_json)
            else:
                credentials_dict = credentials_json
            
            scopes = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            creds = Credentials.from_service_account_info(credentials_dict, scopes=scopes)
            self.client = gspread.authorize(creds)
            self.spreadsheet = self.client.open_by_key(spreadsheet_id)
            logger.info(f"Google Sheets подключен (ID: {spreadsheet_id})")
        except Exception as e:
            logger.error(f"Ошибка подключения к Google Sheets: {e}", exc_info=True)
            self.client = None
            self.spreadsheet = None
    
    def _check_rate_limit(self, priority: int = PRIORITY_HIGH) -> bool:
        """
        Проверить, можно ли выполнить запрос с учетом лимитов API
        
        Args:
            priority: Приоритет операции (PRIORITY_HIGH или PRIORITY_LOW)
            
        Returns:
            True если можно выполнить запрос, False если нужно пропустить
        """
        now = time.time()
        
        # Удаляем старые запросы (старше окна времени)
        while self.request_times and self.request_times[0] < now - API_TIME_WINDOW:
            self.request_times.popleft()
        
        # Для низкоприоритетных запросов (логи) используем более строгий лимит
        # Оставляем запас для высокоприоритетных запросов
        low_priority_limit = int(API_RATE_LIMIT * 0.3)  # 30% лимита для логов
        high_priority_limit = API_RATE_LIMIT
        
        if priority == PRIORITY_LOW:
            # Для логов - пропускаем, если уже много запросов
            if len(self.request_times) >= low_priority_limit:
                logger.debug(f"Пропущен низкоприоритетный запрос (логи) из-за лимита API ({len(self.request_times)}/{low_priority_limit} запросов)")
                return False
        
        # Если запросов меньше лимита - можно выполнить
        if len(self.request_times) < high_priority_limit:
            return True
        
        # Если запросов достигнут лимит:
        # - Высокоприоритетные запросы выполняем (можем превысить лимит, но это риск)
        # - Низкоприоритетные запросы (логи) пропускаем
        if priority == PRIORITY_HIGH:
            # Для критичных данных - выполняем, но логируем предупреждение
            logger.warning(f"Достигнут лимит API ({len(self.request_times)} запросов), но выполняем высокоприоритетный запрос")
            return True
        else:
            # Для логов - пропускаем
            logger.debug(f"Пропущен низкоприоритетный запрос (логи) из-за лимита API ({len(self.request_times)} запросов)")
            return False
    
    def _record_request(self):
        """Записать время выполнения запроса"""
        self.request_times.append(time.time())
    
    def is_available(self) -> bool:
        """Проверить, доступен ли Google Sheets"""
        return self.client is not None and self.spreadsheet is not None
    
    def get_worksheet(self, name: str, create_if_missing: bool = True):
        """Получить лист по имени, создать если не существует"""
        if not self.is_available():
            return None
        
        try:
            return self.spreadsheet.worksheet(name)
        except gspread.exceptions.WorksheetNotFound:
            if create_if_missing:
                return self.spreadsheet.add_worksheet(title=name, rows=1000, cols=20)
            return None
    
    def read_all_rows(self, worksheet_name: str, priority: int = PRIORITY_HIGH) -> List[List[str]]:
        """
        Прочитать все строки из листа
        
        Args:
            worksheet_name: Имя листа
            priority: Приоритет операции (PRIORITY_HIGH или PRIORITY_LOW)
        """
        if not self.is_available():
            return []
        
        if not self._check_rate_limit(priority):
            return []
        
        worksheet = self.get_worksheet(worksheet_name)
        if not worksheet:
            return []
        
        try:
            result = worksheet.get_all_values()
            self._record_request()
            return result
        except Exception as e:
            logger.error(f"Ошибка чтения из {worksheet_name}: {e}")
            return []
    
    def write_rows(self, worksheet_name: str, rows: List[List[str]], clear_first: bool = True, priority: int = PRIORITY_HIGH):
        """
        Записать строки в лист
        
        Args:
            worksheet_name: Имя листа
            rows: Список строк для записи
            clear_first: Очистить лист перед записью
            priority: Приоритет операции (PRIORITY_HIGH или PRIORITY_LOW)
        """
        if not self.is_available():
            return False
        
        if not self._check_rate_limit(priority):
            return False
        
        worksheet = self.get_worksheet(worksheet_name)
        if not worksheet:
            return False
        
        try:
            if clear_first:
                worksheet.clear()
                self._record_request()
            if rows:
                worksheet.update(rows, value_input_option='RAW')
                self._record_request()
            return True
        except gspread.exceptions.APIError as e:
            # Обработка ошибки 429 (превышение лимита)
            if e.response.status_code == 429:
                # Добавляем в буфер для повторной попытки
                self._add_to_buffer(OperationType.WRITE_ROWS, worksheet_name, {'rows': rows, 'clear_first': clear_first}, priority)
                if priority == PRIORITY_LOW:
                    return False
                else:
                    logger.warning(f"Превышен лимит API при записи в {worksheet_name}, добавлено в буфер")
                    return False
            else:
                logger.error(f"Ошибка записи в {worksheet_name}: {e}")
                return False
        except Exception as e:
            logger.error(f"Ошибка записи в {worksheet_name}: {e}")
            return False
    
    def append_row(self, worksheet_name: str, row: List[str], priority: int = PRIORITY_HIGH):
        """
        Добавить строку в конец листа
        
        Args:
            worksheet_name: Имя листа
            row: Строка для добавления
            priority: Приоритет операции (PRIORITY_HIGH или PRIORITY_LOW)
        """
        if not self.is_available():
            return False
        
        if not self._check_rate_limit(priority):
            return False
        
        worksheet = self.get_worksheet(worksheet_name)
        if not worksheet:
            return False
        
        try:
            worksheet.append_row(row, value_input_option='RAW')
            self._record_request()
            return True
        except gspread.exceptions.APIError as e:
            # Обработка ошибки 429 (превышение лимита)
            if e.response.status_code == 429:
                # Добавляем в буфер для повторной попытки
                self._add_to_buffer(OperationType.APPEND_ROW, worksheet_name, row, priority)
                if priority == PRIORITY_LOW:
                    # Для логов просто пропускаем, не логируем ошибку
                    return False
                else:
                    # Для высокоприоритетных запросов логируем предупреждение
                    logger.warning(f"Превышен лимит API при добавлении строки в {worksheet_name}, добавлено в буфер")
                    return False
            else:
                logger.error(f"Ошибка добавления строки в {worksheet_name}: {e}")
                return False
        except Exception as e:
            logger.error(f"Ошибка добавления строки в {worksheet_name}: {e}")
            return False
    
    def _add_to_buffer(self, operation_type: OperationType, worksheet_name: str, data: Any, priority: int):
        """Добавить операцию в буфер для повторной попытки"""
        buffered_op = BufferedOperation(
            operation_type=operation_type,
            worksheet_name=worksheet_name,
            data=data,
            priority=priority,
            timestamp=time.time()
        )
        self.operation_buffer.append(buffered_op)
        logger.debug(f"Операция {operation_type.value} для {worksheet_name} добавлена в буфер (размер: {len(self.operation_buffer)})")
    
    def has_buffered_operations_for_sheet(self, worksheet_name: str) -> bool:
        """
        Проверить, есть ли буферизованные операции для указанного листа
        
        Args:
            worksheet_name: Имя листа
            
        Returns:
            True если есть буферизованные операции для этого листа, False иначе
        """
        if not hasattr(self, 'operation_buffer'):
            return False
        return any(op.worksheet_name == worksheet_name for op in self.operation_buffer)
    
    async def _flush_operation_buffer(self):
        """
        Периодическая задача для отправки буферизованных операций
        Вызывается каждые 60 секунд
        """
        while True:
            try:
                await asyncio.sleep(BUFFER_RETRY_INTERVAL)
                
                # Проверяем, есть ли что отправлять
                if not self.operation_buffer:
                    continue
                
                if not self.is_available():
                    continue
                
                # Сортируем операции по приоритету (высокоприоритетные сначала)
                sorted_ops = sorted(self.operation_buffer, key=lambda x: (x.priority, x.timestamp), reverse=True)
                
                # Пытаемся отправить операции
                sent_count = 0
                failed_count = 0
                
                for op in sorted_ops:
                    # Проверяем лимит перед каждой операцией
                    if not self._check_rate_limit(op.priority):
                        # Если лимит достигнут, останавливаемся
                        failed_count = len([o for o in self.operation_buffer if o not in sorted_ops[:sent_count]])
                        break
                    
                    success = False
                    try:
                        if op.operation_type == OperationType.APPEND_ROW:
                            worksheet = self.get_worksheet(op.worksheet_name)
                            if worksheet:
                                worksheet.append_row(op.data, value_input_option='RAW')
                                self._record_request()
                                success = True
                        elif op.operation_type == OperationType.WRITE_ROWS:
                            worksheet = self.get_worksheet(op.worksheet_name)
                            if worksheet:
                                if op.data.get('clear_first', True):
                                    worksheet.clear()
                                    self._record_request()
                                if op.data.get('rows'):
                                    worksheet.update(op.data['rows'], value_input_option='RAW')
                                    self._record_request()
                                success = True
                        elif op.operation_type == OperationType.UPDATE_ROW:
                            worksheet = self.get_worksheet(op.worksheet_name)
                            if worksheet:
                                all_rows = worksheet.get_all_values()
                                self._record_request()
                                for i, row in enumerate(all_rows, start=1):
                                    if len(row) > op.data['search_col'] and str(row[op.data['search_col']]) == str(op.data['search_value']):
                                        worksheet.update(f'A{i}', [op.data['new_row']], value_input_option='RAW')
                                        self._record_request()
                                        success = True
                                        break
                        elif op.operation_type == OperationType.SET_CELL:
                            worksheet = self.get_worksheet(op.worksheet_name)
                            if worksheet:
                                worksheet.update(op.data['cell'], op.data['value'], value_input_option='RAW')
                                self._record_request()
                                success = True
                        
                        if success:
                            # Удаляем операцию из буфера
                            if op in self.operation_buffer:
                                self.operation_buffer.remove(op)
                            sent_count += 1
                        else:
                            failed_count += 1
                    except gspread.exceptions.APIError as e:
                        if e.response.status_code == 429:
                            # Лимит все еще превышен, оставляем в буфере
                            failed_count += 1
                            break  # Прекращаем попытки
                        else:
                            # Другая ошибка - удаляем из буфера, чтобы не зациклиться
                            if op in self.operation_buffer:
                                self.operation_buffer.remove(op)
                            logger.error(f"Ошибка при повторной отправке операции {op.operation_type.value}: {e}")
                    except Exception as e:
                        # Другая ошибка - удаляем из буфера
                        if op in self.operation_buffer:
                            self.operation_buffer.remove(op)
                        logger.error(f"Ошибка при повторной отправке операции {op.operation_type.value}: {e}")
                
                if sent_count > 0:
                    logger.info(f"Отправлено {sent_count} операций из буфера (осталось: {failed_count})")
            except Exception as e:
                logger.error(f"Ошибка при отправке буферизованных операций: {e}")
    
    def start_buffer_flusher(self):
        """Запустить задачу для периодической отправки буферизованных операций"""
        if self._buffer_flusher_task is None or self._buffer_flusher_task.done():
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    self._buffer_flusher_task = asyncio.create_task(self._flush_operation_buffer())
                    logger.info("Запущена задача для отправки буферизованных операций")
            except RuntimeError:
                # Если нет event loop, создадим задачу позже
                pass
    
    def find_and_update_row(self, worksheet_name: str, search_col: int, search_value: str, new_row: List[str], priority: int = PRIORITY_HIGH):
        """Найти строку по значению в колонке и обновить её"""
        if not self.is_available():
            return False
        
        if not self._check_rate_limit(priority):
            return False
        
        worksheet = self.get_worksheet(worksheet_name)
        if not worksheet:
            return False
        
        try:
            all_rows = worksheet.get_all_values()
            self._record_request()
            for i, row in enumerate(all_rows, start=1):
                if len(row) > search_col and str(row[search_col]) == str(search_value):
                    # Обновляем строку
                    worksheet.update(f'A{i}', [new_row], value_input_option='RAW')
                    self._record_request()
                    return True
            return False
        except gspread.exceptions.APIError as e:
            # Обработка ошибки 429 (превышение лимита)
            if e.response.status_code == 429:
                # Добавляем в буфер для повторной попытки
                self._add_to_buffer(OperationType.UPDATE_ROW, worksheet_name, {'search_col': search_col, 'search_value': search_value, 'new_row': new_row}, priority)
                if priority == PRIORITY_LOW:
                    return False
                else:
                    logger.warning(f"Превышен лимит API при обновлении строки в {worksheet_name}, добавлено в буфер")
                    return False
            else:
                logger.error(f"Ошибка обновления строки в {worksheet_name}: {e}")
                return False
        except Exception as e:
            logger.error(f"Ошибка обновления строки в {worksheet_name}: {e}")
            return False
    
    def find_and_delete_row(self, worksheet_name: str, search_col: int, search_value: str, priority: int = PRIORITY_HIGH):
        """Найти строку по значению в колонке и удалить её"""
        if not self.is_available():
            return False
        
        if not self._check_rate_limit(priority):
            return False
        
        worksheet = self.get_worksheet(worksheet_name)
        if not worksheet:
            return False
        
        try:
            all_rows = worksheet.get_all_values()
            self._record_request()
            for i, row in enumerate(all_rows, start=1):
                if len(row) > search_col and str(row[search_col]) == str(search_value):
                    worksheet.delete_rows(i)
                    self._record_request()
                    return True
            return False
        except Exception as e:
            logger.error(f"Ошибка удаления строки в {worksheet_name}: {e}")
            return False
    
    def get_cell_value(self, worksheet_name: str, cell: str, priority: int = PRIORITY_HIGH) -> Optional[str]:
        """Получить значение ячейки"""
        if not self.is_available():
            return None
        
        if not self._check_rate_limit(priority):
            return None
        
        worksheet = self.get_worksheet(worksheet_name)
        if not worksheet:
            return None
        
        try:
            result = worksheet.acell(cell).value
            self._record_request()
            return result
        except Exception as e:
            logger.error(f"Ошибка чтения ячейки {cell} из {worksheet_name}: {e}")
            return None
    
    def set_cell_value(self, worksheet_name: str, cell: str, value: str, priority: int = PRIORITY_HIGH):
        """Установить значение ячейки"""
        if not self.is_available():
            return False
        
        if not self._check_rate_limit(priority):
            return False
        
        worksheet = self.get_worksheet(worksheet_name)
        if not worksheet:
            return False
        
        try:
            worksheet.update(cell, value, value_input_option='RAW')
            self._record_request()
            return True
        except gspread.exceptions.APIError as e:
            # Обработка ошибки 429 (превышение лимита)
            if e.response.status_code == 429:
                # Добавляем в буфер для повторной попытки
                self._add_to_buffer(OperationType.SET_CELL, worksheet_name, {'cell': cell, 'value': value}, priority)
                if priority == PRIORITY_LOW:
                    return False
                else:
                    logger.warning(f"Превышен лимит API при записи ячейки {cell} в {worksheet_name}, добавлено в буфер")
                    return False
            else:
                logger.error(f"Ошибка записи ячейки {cell} в {worksheet_name}: {e}")
                return False
        except Exception as e:
            logger.error(f"Ошибка записи ячейки {worksheet_name}: {e}")
            return False
