FROM python:3.11-slim

# Переходим в папку проекта
WORKDIR /app/VPN-main

# Устанавливаем зависимости
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir .

# Добавляем папку src в пути поиска Python, чтобы он видел пакет shop_bot
ENV PYTHONPATH="/app/VPN-main/src:${PYTHONPATH}"

# Запуск модуля через флаг -m
# Это эквивалентно запуску файла src/shop_bot/__main__.py
CMD ["python", "-m", "shop_bot"]
