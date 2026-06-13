# Берём официальный готовый образ Airflow
FROM apache/airflow:2.7.1-python3.10

# Переключаемся под пользователя root, чтобы иметь права на системные обновления (если понадобятся)
USER root

# Обновляем системные пакеты и ставим компилятор GCC (он бывает нужен для сборки некоторых питоновских либ типа psycopg2)
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Возвращаемся обратно под стандартного пользователя airflow (безопасность превыше всего)
USER airflow

# Обновляем сам pip внутри контейнера
RUN pip install --no-cache-dir --upgrade pip

# Устанавливаем все библиотеки, которые мы используем в наших четырех скриптах.
# Флаг --no-cache-dir уменьшает финальный вес нашего Docker-образа
RUN pip install --no-cache-dir \
    pandas \
    faker \
    sqlalchemy \
    psycopg2-binary \
    python-dotenv