import os  # Импортируем модуль для работы с операционной системой
import psycopg2  # Импортируем драйвер для подключения к базе данных PostgreSQL
from dotenv import load_dotenv  # Импортируем функцию загрузки переменных из файла .env

# Динамически определяем абсолютный путь к папке, где лежит текущий скрипт
current_dir = os.path.dirname(os.path.abspath(__file__))
# Загружаем конфигурационный файл .env, если он находится в этой же папке
load_dotenv(os.path.join(current_dir, '.env'))

def build_product_sales_mart():
    # Выводим в лог Airflow информационное сообщение о старте процесса
    print("Запуск расчёта витрины: mart.product_sales...")
    
    # Устанавливаем прямое сетевое подключение к контейнеру базы данных внутри сети Docker
    conn = psycopg2.connect(
        host="postgres",     # Имя сервиса базы данных, заданное в docker-compose.yml
        port="5432",         # Стандартный внутренний порт СУБД PostgreSQL
        user="postgres",     # Имя системного пользователя базы данных
        password="postgres", # Пароль для доступа к СУБД
        database="shop_db"   # Имя целевой базы данных интернет-магазина
    )
    
    try:
        # Отключаем автоматический коммит, чтобы управлять транзакцией вручную (надежность данных)
        conn.autocommit = False 
        
        # Формируем SQL-запрос для построения продуктовой витрины
        query = """
        -- Создаем схему mart для хранения витрин, если она не была создана ранее
        CREATE SCHEMA IF NOT EXISTS mart;
        
        -- Удаляем старую таблицу витрины, чтобы избежать конфликтов при перезапуске конвейера
        DROP TABLE IF EXISTS mart.product_sales;

        -- Создаем новую таблицу витрины на лету на основе результирующего SELECT-запроса (CTAS)
        CREATE TABLE mart.product_sales AS
        
        -- CTE 1: Расчет средних дневных продаж товаров за последнее актуальное окно в 30 дней
        WITH daily_sales_last_month AS (
            SELECT 
                oi.product_id,                           -- Идентификатор товара
                SUM(oi.quantity) / 30.0 AS avg_daily_sales -- Суммируем проданное количество и делим на 30 дней
            FROM public.order_items oi                  -- Таблица с позициями чеков/заказов
            JOIN public.orders o ON oi.order_id = o.order_id -- Объединяем с общей таблицей заказов по ID
            -- Фильтруем данные: приводим текстовую дату к типу date, отступаем 30 дней назад и убираем отмены
            WHERE o.order_date::date >= '2026-06-01'::date - INTERVAL '30 days' 
              AND o.status != 'Cancelled'
            GROUP BY oi.product_id                       -- Группируем результат в разрезе каждого товара
        ),
        
        -- CTE 2: Расчет базовых агрегатов продаж, выручки и определение Lead Time по категориям
        product_metrics AS (
            SELECT 
                p.product_id,                            -- Идентификатор товара
                p.name,                                  -- Наименование товара
                p.category,                              -- Категория товара
                p.stock AS stock_remaining,              -- Текущий фактический остаток на складе
                COALESCE(SUM(oi.quantity), 0) AS total_quantity_sold, -- Общее кол-во продаж (заменяем NULL на 0)
                COALESCE(SUM(oi.quantity * oi.price_at_order), 0) AS total_revenue, -- Выручка с учетом исторических цен
                -- Вычисляем Lead Time (срок поставки в днях) на основании категориальных признаков
                CASE 
                    WHEN p.category = 'Электроника' THEN 5
                    WHEN p.category = 'Одежда' THEN 7
                    ELSE 4
                END AS lead_time_days
            FROM public.products p                      -- Базовый справочник товаров
            -- Используем LEFT JOIN, чтобы в витрину попали даже те товары, у которых еще нет ни одной продажи
            LEFT JOIN public.order_items oi ON p.product_id = oi.product_id
            -- Привязываем заказы и сразу отсекаем отмененные статусы, чтобы они не раздували метрики выручки
            LEFT JOIN public.orders o ON oi.order_id = o.order_id AND o.status != 'Cancelled'
            GROUP BY p.product_id, p.name, p.category, p.stock -- Группировка для корректной агрегации функций SUM
        ),
        
        -- CTE 3: Расчет средней оценки качества товара на основании отзывов клиентов
        ratings AS (
            SELECT 
                product_id, 
                ROUND(AVG(rating)::numeric, 2) AS avg_rating -- Считаем среднее и округляем строго до 2 знаков
            FROM public.reviews                         -- Таблица с клиентскими отзывами
            GROUP BY product_id                          -- Группируем оценки по товарам
        )
        
        -- Финальный шаг: Джойним все подготовленные блоки данных в единую структуру витрины
        SELECT 
            pm.product_id,                               -- ID товара
            pm.name,                                     -- Название
            pm.category,                                 -- Категория
            pm.total_quantity_sold,                      -- Сколько всего штук продано
            pm.total_revenue,                            -- Сгенерированная выручка в рублях
            COALESCE(r.avg_rating, 0) AS avg_rating,     -- Средний рейтинг (если отзывов нет — ставим 0)
            pm.stock_remaining,                          -- Остаток на складе
            -- Считаем Reorder Level (точку заказа): средние продажи умножаем на срок поставки и округляем вверх (CEIL)
            COALESCE(CEIL(dslm.avg_daily_sales * pm.lead_time_days), 0)::integer AS reorder_level
        FROM product_metrics pm                          -- За основу берем метрики продуктов
        LEFT JOIN ratings r ON pm.product_id = r.product_id -- Притягиваем средний рейтинг
        LEFT JOIN daily_sales_last_month dslm ON pm.product_id = dslm.product_id; -- Притягиваем темп продаж

        -- Добавляем первичный ключ (Primary Key) на витрину для связей и ускорения будущих BI-отчетов
        ALTER TABLE mart.product_sales ADD PRIMARY KEY (product_id);
        """
        
        # Создаем контекстный менеджер курсора для выполнения операций в базе данных
        with conn.cursor() as cursor:
            cursor.execute(query) # Отправляем собранный SQL-запрос на исполнение в СУБД
            
        conn.commit() # Если ошибок синтаксиса или типов не возникло — фиксируем транзакцию в БД
        print("Витрина mart.product_sales успешно обновлена.") # Выводим отчет об успешном завершении
        
    except Exception as e:
        conn.rollback() # В случае падения на любом этапе полностью откатываем транзакцию (Data Integrity)
        raise e # Пробрасываем ошибку дальше, чтобы Airflow зафиксировал статус Task Failed
        
    finally:
        conn.close() # В обязательном порядке закрываем сессию подключения, освобождая пулы СУБД

# Точка входа: запускает функцию построения витрины при прямом вызове файла
if __name__ == "__main__":
    build_product_sales_mart()