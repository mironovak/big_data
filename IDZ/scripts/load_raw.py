import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Загружаем переменные из примонтированного файла .env
import sys
# Определяем папку, в которой лежит этот скрипт, и ищем .env рядом
current_dir = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.join(current_dir, '.env')

if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
else:
    # Если файла .env в папке scripts нет, берем дефолтные значения напрямую для Docker-сети
    os.environ.setdefault('DB_HOST', 'postgres')
    os.environ.setdefault('DB_PORT', '5432')
    os.environ.setdefault('DB_USER', 'postgres')
    os.environ.setdefault('DB_PASSWORD', 'postgres')
    os.environ.setdefault('DB_NAME', 'shop_db')

def load_csv_to_db():
    print("Подключение к PostgreSQL...")
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT')
    db = os.getenv('DB_NAME')

    # Инициализируем движок SQLAlchemy для работы Pandas с СУБД
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    data_dir = '/opt/airflow/data'

    # engine.begin() автоматически открывает транзакцию и делает commit в конце (или rollback при ошибке)
    with engine.begin() as conn:
        print("Подготовка изолированных схем (public и mart)...")
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS public;"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS mart;"))
        
        print("Удаление старых объектов базы данных (Идемпотентность)...")
        # CASCADE принудительно удаляет таблицы, даже если на них ссылаются внешние ключи
        conn.execute(text("DROP TABLE IF EXISTS mart.product_sales CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS mart.customer_value CASCADE;"))
        conn.execute(text("""
            DROP TABLE IF EXISTS 
                public.reviews, public.order_items, 
                public.orders, public.products, public.customers 
            CASCADE;
        """))

    # Порядок критически важен из-за будущих Foreign Keys: сначала родители (customers, products), потом дети
    tables = ['customers', 'products', 'orders', 'order_items', 'reviews']
    for t in tables:
        file_path = os.path.join(data_dir, f'{t}.csv')
        df = pd.read_csv(file_path)
        
        # to_sql — метод Pandas, который пачками заливает DataFrame в СУБД
        df.to_sql(
            t, 
            engine, 
            schema='public', 
            if_exists='append', # вставляем строки в чистую таблицу
            index=False
        )
        print(f"Таблица public.{t} успешно наполнена данными.")

    # Добавляем индексы и связи (Ограничения целостности), так как Pandas сам их делать не умеет
    with engine.begin() as conn:
        print("Добавление первичных (PK) и внешних ключей (FK)...")
        conn.execute(text("ALTER TABLE public.customers ADD PRIMARY KEY (customer_id);"))
        conn.execute(text("ALTER TABLE public.products ADD PRIMARY KEY (product_id);"))
        conn.execute(text("ALTER TABLE public.orders ADD PRIMARY KEY (order_id);"))
        conn.execute(text("ALTER TABLE public.order_items ADD PRIMARY KEY (item_id);"))
        conn.execute(text("ALTER TABLE public.reviews ADD PRIMARY KEY (review_id);"))
        
        # Навешиваем внешние ключи (Связи между сущностями онтологии)
        conn.execute(text("ALTER TABLE public.orders ADD CONSTRAINT fk_orders_cust FOREIGN KEY (customer_id) REFERENCES public.customers(customer_id);"))
        conn.execute(text("ALTER TABLE public.order_items ADD CONSTRAINT fk_items_order FOREIGN KEY (order_id) REFERENCES public.orders(order_id);"))
        conn.execute(text("ALTER TABLE public.order_items ADD CONSTRAINT fk_items_prod FOREIGN KEY (product_id) REFERENCES public.products(product_id);"))
        conn.execute(text("ALTER TABLE public.reviews ADD CONSTRAINT fk_rev_prod FOREIGN KEY (product_id) REFERENCES public.products(product_id);"))
    
    print("Загрузка Staging-слоя завершена.")

if __name__ == "__main__":
    load_csv_to_db()