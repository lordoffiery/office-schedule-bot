-- Расширение default_schedule с 8 до 11 мест (1.9, 1.10, 1.11 — свободные).
-- Выполнить в psql или клиенте PostgreSQL после деплоя кода с MAX_OFFICE_SEATS = 11.
-- Данные places_json совпадают с текущим продом на момент миграции; при расхождении
-- лучше использовать scripts/extend_default_schedule_to_11_seats.py (мержит по факту из БД).

UPDATE default_schedule SET places_json = $${"1.1": "Дима Ч(@dmitrii_chep)", "1.2": "Тимур(@ttimg2)", "1.3": "Вася(@vbronsky)", "1.4": "Илья(@Ilyastepankov)", "1.5": "Егор(@trueuser)", "1.6": "Айлар(@Ayab88)", "1.7": "Даша(@thesavva)", "1.8": "", "1.9": "", "1.10": "", "1.11": ""}$$, updated_at = NOW() WHERE day_name = 'Понедельник';

UPDATE default_schedule SET places_json = $${"1.1": "Дима Ч(@dmitrii_chep)", "1.2": "Тимур(@ttimg2)", "1.3": "Вася(@vbronsky)", "1.4": "Айдан(@Aydaaannnnn)", "1.5": "Рома(@rsidorenkov)", "1.6": "Дима А(@almyash)", "1.7": "Костя(@BKY89)", "1.8": "Леша Б(@aab_must)", "1.9": "", "1.10": "", "1.11": ""}$$, updated_at = NOW() WHERE day_name = 'Вторник';

UPDATE default_schedule SET places_json = $${"1.1": "Дима Ч(@dmitrii_chep)", "1.2": "Тимур(@ttimg2)", "1.3": "Костя(@BKY89)", "1.4": "Илья(@Ilyastepankov)", "1.5": "Рома(@rsidorenkov)", "1.6": "", "1.7": "", "1.8": "Марк(@MarkSuDm)", "1.9": "", "1.10": "", "1.11": ""}$$, updated_at = NOW() WHERE day_name = 'Среда';

UPDATE default_schedule SET places_json = $${"1.1": "Дима Ч(@dmitrii_chep)", "1.2": "Тимур(@ttimg2)", "1.3": "Вася(@vbronsky)", "1.4": "Леша Б(@aab_must)", "1.5": "Даша_Т(@daryatrr)", "1.6": "Марк(@MarkSuDm)", "1.7": "Толя(@tolifer)", "1.8": "Глеб(@acpllsng)", "1.9": "", "1.10": "", "1.11": ""}$$, updated_at = NOW() WHERE day_name = 'Четверг';

UPDATE default_schedule SET places_json = $${"1.1": "Дима Ч(@dmitrii_chep)", "1.2": "Тимур(@ttimg2)", "1.3": "Вася(@vbronsky)", "1.4": "Илья(@Ilyastepankov)", "1.5": "Егор(@trueuser)", "1.6": "Айлар(@Ayab88)", "1.7": "Даша(@thesavva)", "1.8": "Даша_Т(@daryatrr)", "1.9": "", "1.10": "", "1.11": ""}$$, updated_at = NOW() WHERE day_name = 'Пятница';
