# Используем официальный Python образ
FROM python:3.11-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# Устанавливаем системные зависимости (если нужны)
# RUN apt-get update && apt-get install -y \
#     && rm -rf /var/lib/apt/lists/*

# Копируем файл зависимостей
COPY requirements.txt .

# Устанавливаем Python зависимости
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Копируем весь код проекта
COPY *.py ./

# Создаем директории для данных (если нужны для fallback)
# init_data.py создаст их автоматически при запуске, но создаем заранее для безопасности
RUN mkdir -p data/schedules data/requests data/queue

# Устанавливаем переменные окружения (по умолчанию)
ENV PYTHONUNBUFFERED=1
ENV USE_GOOGLE_SHEETS=true

# Запускаем бота
CMD ["python", "main.py"]

