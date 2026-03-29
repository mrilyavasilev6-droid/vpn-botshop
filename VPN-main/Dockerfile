FROM python:3.11-slim

# Устанавливаем рабочую директорию внутри контейнера
WORKDIR /app

# Копируем всё содержимое папки VPN-main прямо в /app
# Точка означает "текущая папка, где лежит Dockerfile"
COPY . .

# Обновляем pip и устанавливаем проект (включая aiosqlite из pyproject.toml)
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir .

# Запуск бота. 
# ВНИМАНИЕ: Если ваш главный файл называется main.py, замените bot.py ниже
CMD ["python", "bot.py"]
