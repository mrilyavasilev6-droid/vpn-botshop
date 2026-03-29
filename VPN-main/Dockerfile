FROM python:3.11-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем всё из текущей папки (где лежит Dockerfile) в /app
COPY . /app/

# Обновляем pip
RUN pip install --no-cache-dir --upgrade pip

# Устанавливаем зависимости из pyproject.toml, который теперь точно в /app
RUN pip install --no-cache-dir .

# Запуск бота
# Проверьте, что в папке VPN-main главный файл называется именно bot.py
# Если он называется по-другому, замените "bot.py" на правильное имя
CMD ["python", "bot.py"]
