FROM python:3.11-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# 1. КОПИРУЕМ все файлы из репозитория в образ (ЭТОТ ШАГ БЫЛ ПРОПУЩЕН)
COPY . .

# 2. Переходим в папку, где лежит pyproject.toml
WORKDIR /app/VPN-main

# 3. Обновляем pip и устанавливаем проект
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir .

# 4. Настраиваем пути поиска Python
ENV PYTHONPATH="/app/VPN-main/src:${PYTHONPATH}"

# 5. Запускаем модуль
CMD ["python", "-m", "shop_bot"]
