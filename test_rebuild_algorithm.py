#!/usr/bin/env python3
"""
Тестовый скрипт для воспроизведения алгоритма rebuild_schedules_from_requests
"""
import json
from datetime import datetime, timedelta
from typing import Dict, List

# Входные данные из default_schedule
default_schedule = {
    "Понедельник": {"1.1": "Дима Ч(@dmitrii_chep)", "1.2": "Тимур(@ttimg2)", "1.3": "Вася(@vbronsky)", "1.4": "Илья(@Ilyastepankov)", "1.5": "Егор(@trueuser)", "1.6": "Айлар(@Ayab88)", "1.7": "Даша(@thesavva)", "1.8": ""},
    "Вторник": {"1.1": "Дима Ч(@dmitrii_chep)", "1.2": "Тимур(@ttimg2)", "1.3": "Вася(@vbronsky)", "1.4": "Айдан(@Aydaaannnnn)", "1.5": "Рома(@rsidorenkov)", "1.6": "Дима А(@almyash)", "1.7": "Костя(@BKY89)", "1.8": "Леша Б(@aab_must)"},
    "Среда": {"1.1": "Дима Ч(@dmitrii_chep)", "1.2": "Тимур(@ttimg2)", "1.3": "Костя(@BKY89)", "1.4": "Илья(@Ilyastepankov)", "1.5": "Рома(@rsidorenkov)", "1.6": "Катя(@kylia226)", "1.7": "Артем(@no_pasaran_001)", "1.8": "Марк(@MarkSuDm)"},
    "Четверг": {"1.1": "Дима Ч(@dmitrii_chep)", "1.2": "Тимур(@ttimg2)", "1.3": "Вася(@vbronsky)", "1.4": "Леша Б(@aab_must)", "1.5": "Рома(@rsidorenkov)", "1.6": "Марк(@MarkSuDm)", "1.7": "Толя(@tolifer)", "1.8": "Глеб(@acpllsng)"},
    "Пятница": {"1.1": "Дима Ч(@dmitrii_chep)", "1.2": "Тимур(@ttimg2)", "1.3": "Вася(@vbronsky)", "1.4": "Илья(@Ilyastepankov)", "1.5": "Егор(@trueuser)", "1.6": "Айлар(@Ayab88)", "1.7": "Даша(@thesavva)", "1.8": ""}
}

# Входные данные из requests
requests_data = [
    {"week_start": "2025-12-22", "employee_name": "Рома", "telegram_id": 312551109, "days_requested": ["Вторник"], "days_skipped": ["Среда", "Четверг", "Понедельник"]},
    {"week_start": "2025-12-29", "employee_name": "Рома", "telegram_id": 312551109, "days_requested": [], "days_skipped": ["Понедельник"]},
    {"week_start": "2026-12-14", "employee_name": "Глеб", "telegram_id": 140036070, "days_requested": [], "days_skipped": ["Вторник"]}
]

def get_plain_name_from_formatted(formatted_name: str) -> str:
    """Извлечь простое имя из отформатированного (например, 'Вася(@vbronsky)' -> 'Вася')"""
    if not formatted_name:
        return ""
    # Убираем никнейм в скобках
    if '(' in formatted_name:
        return formatted_name.split('(')[0].strip()
    return formatted_name.strip()

def get_employees_list_from_places(places_dict: Dict[str, str]) -> List[str]:
    """Получить список имен сотрудников из словаря мест (отсортированный по месту)"""
    sorted_places = sorted(places_dict.items(), key=lambda x: (int(x[0].split('.')[0]), int(x[0].split('.')[1])))
    return [name for _, name in sorted_places if name]

def find_employee_in_places(places_dict: Dict[str, str], employee_name: str) -> str:
    """Найти сотрудника в словаре мест и вернуть ключ места"""
    plain_name = get_plain_name_from_formatted(employee_name)
    for place_key, name in places_dict.items():
        if name and get_plain_name_from_formatted(name) == plain_name:
            return place_key
    return None

def find_free_place(places_dict: Dict[str, str], max_seats: int = 8) -> str:
    """Найти свободное место в словаре мест"""
    for i in range(1, max_seats + 1):
        place_key = f'1.{i}'
        if place_key not in places_dict or not places_dict.get(place_key):
            return place_key
    return None

def assign_fixed_places(default_schedule: Dict, schedule: Dict) -> Dict[str, str]:
    """Назначить фиксированные места сотрудникам на основе приоритета"""
    # Собираем всех сотрудников из default_schedule
    employees_info = {}
    
    for day_name, places_dict in default_schedule.items():
        for place_key, name in places_dict.items():
            plain_name = get_plain_name_from_formatted(name)
            if plain_name:
                if plain_name not in employees_info:
                    employees_info[plain_name] = {'days': {}, 'days_count': 0}
                employees_info[plain_name]['days'][day_name] = place_key
    
    # Подсчитываем количество дней для каждого сотрудника
    for employee_name in employees_info:
        employees_info[employee_name]['days_count'] = len(employees_info[employee_name]['days'])
    
    # Сортируем сотрудников по количеству дней (по убыванию)
    sorted_employees = sorted(
        employees_info.items(),
        key=lambda x: (-x[1]['days_count'], int(list(x[1]['days'].values())[0].split('.')[0]) if x[1]['days'] else 999,
                      int(list(x[1]['days'].values())[0].split('.')[1]) if x[1]['days'] else 999, x[0])
    )
    
    # Назначаем фиксированные места
    employee_to_place = {}
    place_to_employee = {}
    
    for employee_name, info in sorted_employees:
        days_dict = info['days']
        # Берем место из первого дня (или наиболее частое место)
        first_day = list(days_dict.keys())[0]
        assigned_place = days_dict[first_day]
        
        # Проверяем конфликты
        if assigned_place in place_to_employee:
            # Место занято - ищем свободное
            for i in range(1, 9):
                candidate_place = f'1.{i}'
                if candidate_place not in place_to_employee:
                    assigned_place = candidate_place
                    break
        
        if assigned_place:
            employee_to_place[employee_name] = assigned_place
            place_to_employee[assigned_place] = employee_name
            # Назначаем место сотруднику во все его дни
            for day in days_dict.keys():
                if day in schedule:
                    schedule[day][assigned_place] = default_schedule[day][days_dict[day]]
    
    return employee_to_place

def build_schedule_from_requests(week_start: str, requests: List[Dict], default_schedule: Dict) -> tuple[Dict[str, List[str]], Dict[str, set]]:
    """Построить расписание на основе заявок"""
    print(f"\n{'='*60}")
    print(f"Обработка недели {week_start}")
    print(f"{'='*60}")
    
    # Отслеживаем, какие сотрудники были удалены через days_skipped для каждого дня
    # Это нужно, чтобы не добавлять их обратно при дополнении
    removed_by_skipped = {}  # {day: set(employee_names)}
    for day_name in ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница']:
        removed_by_skipped[day_name] = set()
    
    # Копируем расписание по умолчанию (но очищаем имена, оставляя только структуру мест)
    schedule = {}
    for day_name, places_dict in default_schedule.items():
        schedule[day_name] = {}
        # Копируем структуру мест из default_schedule
        for place_key in places_dict.keys():
            schedule[day_name][place_key] = ''
        # Дополняем до 8 мест, если мест меньше
        for i in range(1, 9):
            place_key = f'1.{i}'
            if place_key not in schedule[day_name]:
                schedule[day_name][place_key] = ''
    
    # Шаг 1: Заполняем расписание всеми сотрудниками из default_schedule
    # Сначала копируем всех сотрудников из default_schedule в schedule
    for day_name, places_dict in default_schedule.items():
        for place_key, name in places_dict.items():
            if name:  # Если место занято
                schedule[day_name][place_key] = name
    
    # Шаг 2: Назначаем фиксированные места сотрудникам на основе приоритета
    employee_to_place = assign_fixed_places(default_schedule, schedule)
    print(f"\nНазначенные фиксированные места:")
    for emp, place in sorted(employee_to_place.items()):
        print(f"  {emp}: {place}")
    
    print(f"\nРасписание после заполнения из default_schedule:")
    for day_name in ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница']:
        employees = get_employees_list_from_places(schedule[day_name])
        print(f"  {day_name}: {len(employees)} сотрудников - {', '.join(employees[:3])}...")
    
    # Шаг 3: Применяем заявки (skip_day, add_day)
    requests_by_employee = {}
    for req in requests:
        employee_name = req['employee_name']
        requests_by_employee[employee_name] = {
            'days_requested': req['days_requested'],
            'days_skipped': req['days_skipped']
        }
    
    # Обрабатываем заявки
    for employee_name, req_info in requests_by_employee.items():
        days_requested = req_info['days_requested']
        days_skipped = req_info['days_skipped']
        
        print(f"\nОбрабатываем заявку для {employee_name}:")
        print(f"  days_requested: {days_requested}")
        print(f"  days_skipped: {days_skipped}")
        
        # Получаем фиксированное место сотрудника (если есть)
        fixed_place = employee_to_place.get(employee_name)
        print(f"  fixed_place: {fixed_place}")
        
        # Проверяем, где сотрудник был до обработки заявок (из default_schedule)
        employee_in_default_days = set()
        for day_name, places_dict in default_schedule.items():
            for place_key, name in places_dict.items():
                plain_name = get_plain_name_from_formatted(name)
                if plain_name == employee_name:
                    employee_in_default_days.add(day_name)
        print(f"  employee_in_default_days: {employee_in_default_days}")
        
        # Сначала добавляем сотрудника в запрошенные дни
        for day in days_requested:
            if day in schedule and day not in days_skipped:
                place_key = find_employee_in_places(schedule[day], employee_name)
                if not place_key:
                    if fixed_place:
                        if fixed_place not in schedule[day] or not schedule[day].get(fixed_place):
                            schedule[day][fixed_place] = default_schedule[day].get(fixed_place, employee_name)
                            print(f"  ✅ Добавлен {employee_name} в {day} на место {fixed_place}")
                        else:
                            free_place = find_free_place(schedule[day])
                            if free_place:
                                schedule[day][free_place] = employee_name
                                print(f"  ✅ Добавлен {employee_name} в {day} на место {free_place} (место {fixed_place} занято)")
                    else:
                        free_place = find_free_place(schedule[day])
                        if free_place:
                            schedule[day][free_place] = employee_name
                            print(f"  ✅ Добавлен {employee_name} в {day} на место {free_place}")
        
            # Удаляем сотрудника из пропущенных дней
            # days_skipped удаляет сотрудника независимо от того, был ли он в default_schedule или был добавлен через requests
            for day in days_skipped:
                if day in schedule:
                    day_was_requested = day in days_requested
                    day_was_in_default = day in employee_in_default_days
                    
                    print(f"\n  День {day}:")
                    print(f"    был запрошен={day_was_requested}, был в default={day_was_in_default}, пропущен=True")
                    
                    place_key_before = find_employee_in_places(schedule[day], employee_name)
                    print(f"    сотрудник в расписании до удаления: {place_key_before is not None}")
                    
                    # Удаляем сотрудника из дня, если он указал этот день в days_skipped
                    # Независимо от того, был ли он в default_schedule или был добавлен через requests
                    print(f"    ✅ УДАЛЯЕМ {employee_name} из {day} (указан в days_skipped)")
                    if fixed_place and fixed_place in schedule[day]:
                        # Проверяем, что на этом месте именно этот сотрудник
                        place_name = get_plain_name_from_formatted(schedule[day][fixed_place])
                        if place_name == employee_name:
                            schedule[day][fixed_place] = ''
                            removed_by_skipped[day].add(employee_name)
                    else:
                        place_key = find_employee_in_places(schedule[day], employee_name)
                        if place_key:
                            schedule[day][place_key] = ''
                            removed_by_skipped[day].add(employee_name)
                    
                    place_key_after = find_employee_in_places(schedule[day], employee_name)
                    print(f"    сотрудник в расписании после обработки: {place_key_after is not None}")
    
    # Конвертируем обратно в формат списка для вывода
    formatted_schedule = {}
    for day, places_dict in schedule.items():
        employees = get_employees_list_from_places(places_dict)
        formatted_schedule[day] = employees
    
    return formatted_schedule, removed_by_skipped

def format_employee_name(name: str) -> str:
    """Форматировать имя сотрудника (просто возвращаем как есть)"""
    return name

def main():
    print("="*60)
    print("ТЕСТ АЛГОРИТМА rebuild_schedules_from_requests")
    print("="*60)
    
    # Обрабатываем каждую неделю
    for req_data in requests_data:
        week_start = req_data['week_start']
        requests = [req_data]  # Только заявки для этой недели
        
        # Строим расписание
        schedule, removed_by_skipped = build_schedule_from_requests(week_start, requests, default_schedule)
        
        # Форматируем default_schedule для сравнения
        formatted_default = {}
        for day, places_dict in default_schedule.items():
            employees = get_employees_list_from_places(places_dict)
            formatted_default[day] = employees
        
        # Определяем измененные дни
        print(f"\n{'='*60}")
        print(f"Сравнение с default_schedule для недели {week_start}")
        print(f"{'='*60}")
        
        changed_days = set()
        final_schedule = {}
        
        for day_name in ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница']:
            schedule_employees = sorted([e.strip() for e in schedule.get(day_name, []) if e.strip()])
            default_employees = sorted([e.strip() for e in formatted_default.get(day_name, []) if e.strip()])
            
            print(f"\nДень {day_name}:")
            print(f"  schedule до дополнения: {schedule_employees}")
            print(f"  default: {default_employees}")
            print(f"  отличаются до дополнения: {schedule_employees != default_employees}")
            
            if schedule_employees != default_employees:
                schedule_day = schedule.get(day_name, [])
                default_day = formatted_default.get(day_name, [])
                
                schedule_names = set([e.strip() for e in schedule_day if e.strip()])
                
                print(f"  До дополнения: {len(schedule_day)} сотрудников в schedule, {len(default_day)} в default")
                
                # Дополняем пустые места из default
                # НО не добавляем сотрудников, которые были удалены через days_skipped
                for emp in default_day:
                    emp_stripped = emp.strip()
                    emp_plain = get_plain_name_from_formatted(emp_stripped)
                    if emp_stripped and emp_stripped not in schedule_names:
                        # Проверяем, не был ли этот сотрудник удален через days_skipped
                        if emp_plain not in removed_by_skipped.get(day_name, set()):
                            schedule_day.append(emp)
                            schedule_names.add(emp_stripped)
                            if len(schedule_day) >= len(default_day):
                                break
                        else:
                            print(f"  Пропускаем {emp_plain} при дополнении (был удален через days_skipped)")
                
                final_employees = sorted([e.strip() for e in schedule_day if e.strip()])
                print(f"  После дополнения: {final_employees}")
                print(f"  После дополнения отличается от default: {final_employees != default_employees}")
                
                # Сохраняем день, так как он изменился после применения requests
                changed_days.add(day_name)
                final_schedule[day_name] = schedule_day
                print(f"  ✅ День {day_name} будет сохранен (изменился после применения requests)")
            else:
                print(f"  ❌ День {day_name} не изменился после применения requests - не сохраняем")
        
        print(f"\n{'='*60}")
        print(f"РЕЗУЛЬТАТ для недели {week_start}")
        print(f"{'='*60}")
        print(f"Измененные дни: {changed_days}")
        print(f"\nФинальное расписание:")
        for day_name in ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница']:
            if day_name in final_schedule:
                employees = final_schedule[day_name]
                print(f"  {day_name}: {', '.join(employees)}")
            else:
                print(f"  {day_name}: (используется default_schedule)")

if __name__ == "__main__":
    main()

