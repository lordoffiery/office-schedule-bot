# 🔧 Исправление ошибки "Address 127.0.0.1:8080 is not available"

## Проблема

```
Address 127.0.0.1:8080 is not available (dial tcp 127.0.0.1:8080: connect: connection refused), retrying...
```

## Причина

Yandex Cloud Serverless Containers пытается выполнить health check на порту 8080, но наш бот не запускает HTTP-сервер. Это не критичная ошибка, но она может мешать нормальной работе.

## ✅ Решение 1: Отключить Health Check (рекомендуется)

1. Yandex Cloud Console → **Serverless Containers** → ваш контейнер
2. Перейдите на вкладку **Настройки** или **Редактировать**
3. Найдите раздел **Health Check** или **Проверка здоровья**
4. **Отключите Health Check** или установите тип: **Нет проверки**

## ✅ Решение 2: Настроить простой HTTP-сервер для health check

Если нельзя отключить health check, можно добавить простой HTTP-сервер, который будет отвечать на проверки здоровья.

### Вариант A: Добавить простой HTTP-сервер в отдельном потоке

Добавить в `main.py` перед запуском polling:

```python
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'OK')
        else:
            self.send_response(404)
            self.end_headers()

def start_health_server():
    server = HTTPServer(('0.0.0.0', 8080), HealthCheckHandler)
    server.serve_forever()

# В функции main(), перед запуском polling:
health_thread = threading.Thread(target=start_health_server, daemon=True)
health_thread.start()
logger.info("Health check server started on port 8080")
```

### Вариант B: Использовать aiohttp для простого HTTP-сервера

Добавить в `main.py`:

```python
from aiohttp import web

async def health_handler(request):
    return web.Response(text='OK')

# В функции main(), перед запуском polling:
health_app = web.Application()
health_app.router.add_get('/health', health_handler)
health_runner = web.AppRunner(health_app)
await health_runner.setup()
health_site = web.TCPSite(health_runner, '0.0.0.0', 8080)
await health_site.start()
logger.info("Health check server started on port 8080")
```

## ✅ Решение 3: Игнорировать ошибку (если бот работает)

Если бот работает нормально и отвечает на команды, можно просто игнорировать эту ошибку. Она не критична и не влияет на работу бота.

## 🎯 Рекомендация

**Лучший вариант:** Отключить Health Check в настройках контейнера.

Если это невозможно, используйте **Решение 2A** (простой HTTP-сервер в отдельном потоке) - это минимальное изменение кода.

## 📝 Проверка после исправления

1. Перезапустите контейнер
2. Проверьте логи - ошибка должна исчезнуть
3. Проверьте работу бота - должен отвечать на команды

## ⚠️ Важно

- Health check не обязателен для Telegram бота с polling
- Бот работает через long polling к Telegram API, а не через HTTP
- Ошибка health check не влияет на функциональность бота, но может засорять логи

