from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Базовые аргументы для управления поведением тасок при ошибках
default_args = {
    'owner': 'admin',         
    'depends_on_past': False,         
    'start_date': datetime(2026, 1, 1), # Точка отсчета для планировщика (наш текущий год)
    'retries': 1,                       # Количество повторных попыток при падении таски
    'retry_delay': timedelta(minutes=1), # Пауза перед повторным запуском (1 минута)
}

# Инициализируем DAG (направленный ациклический граф)
with DAG(
    dag_id='internet_shop_etl_pipeline',       
    default_args=default_args,
    description='ИДЗ Вариант 5: Пайплайн обработки данных интернет-магазина',
    schedule_interval='@daily',            # Запускать раз в сутки (имитация бизнес-расписания)
    catchup=False,                         # Не нагонять прошлые периоды с start_date при первом старте
    tags=['shop', 'idz', 'bigdata'],    
) as dag:

    # TASK 1: Вызов Python-скрипта через Bash для генерации сырых CSV
    task_generate_data = BashOperator(
        task_id='generate_data',
        bash_command='python /opt/airflow/scripts/generate_data.py',
    )

    # TASK 2: Вызов скрипта загрузки CSV-файлов в схему public нашей СУБД
    task_load_raw = BashOperator(
        task_id='load_raw',
        bash_command='python /opt/airflow/scripts/load_raw.py',
    )

    # TASK 3: Расчет аналитической витрины по товарам
    task_transform_products = BashOperator(
        task_id='create_mart_product_sales',
        bash_command='python /opt/airflow/scripts/product_sales_mart.py',
    )

    # TASK 4: Расчет аналитической витрины по ценности покупателей
    task_transform_customers = BashOperator(
        task_id='create_mart_customer_value',
        bash_command='python /opt/airflow/scripts/customer_value_mart.py',
    )

    # Схема зависимостей (Пайплайн): Сначала генерируем -> затем грузим в public -> затем параллельно собираем витрины
    task_generate_data >> task_load_raw >> [task_transform_products, task_transform_customers]