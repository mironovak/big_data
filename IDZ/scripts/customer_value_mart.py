import os
import psycopg2
from dotenv import load_dotenv

# Находим .env динамически рядом со скриптом
current_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(current_dir, '.env'))

def build_customer_value_mart():
    print("Запуск расчёта витрины: mart.customer_value...")
    
    # Железное подключение к Docker-сети (без дублей функций)
    conn = psycopg2.connect(
        host="postgres",     # Имя сервиса базы данных в docker-compose
        port="5432",         # Внутренний порт базы внутри контейнера
        user="postgres",
        password="postgres",
        database="shop_db"
    )
    
    try:
        conn.autocommit = False
        query = """
        CREATE SCHEMA IF NOT EXISTS mart;
        DROP TABLE IF EXISTS mart.customer_value;

        -- Создаем витрину ценности покупателей (CTAS)
        CREATE TABLE mart.customer_value AS
        SELECT 
            c.customer_id,
            c.full_name,
            -- COUNT(DISTINCT) гарантирует, что мы посчитаем именно уникальные заказы
            COUNT(DISTINCT o.order_id) AS total_orders,
            SUM(o.total_amount) AS total_spent,
            -- Расчет среднего чека покупателя
            ROUND((SUM(o.total_amount)::numeric / COUNT(DISTINCT o.order_id)), 2) AS avg_order_value,
            MAX(o.order_date) AS last_order_date, -- Исправлен комментарий (была решётка)
            -- Расчет Lifetime_days
            ('2026-06-01'::date - c.registration_date::date) AS lifetime_days
        FROM public.customers c
        JOIN public.orders o ON c.customer_id = o.customer_id
        WHERE o.status != 'Cancelled' -- Исключаем отмененные заказы
        GROUP BY c.customer_id, c.full_name, c.registration_date;

        ALTER TABLE mart.customer_value ADD PRIMARY KEY (customer_id);
        """
        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()
        print("Витрина mart.customer_value успешно обновлена.")
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

if __name__ == "__main__":
    build_customer_value_mart()