FROM python:3.11-slim

WORKDIR /app

# Копируем вообще всё, что есть в репозитории
COPY . .

# Переходим именно в ту папку, где лежит ваш pyproject.toml
# Если папка называется VPN-main, то переходим в неё
WORKDIR /app/VPN-main

# Теперь pip точно увидит файл в текущей директории
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir .

# Запуск. Если главный файл лежит в VPN-main, запускаем его
# Если файл называется иначе (например, main.py), замените bot.py
CMD ["python", "bot.py"]
