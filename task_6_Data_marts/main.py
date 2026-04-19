"""
py main.py
SELECT * FROM dmr.analytics_student_performance LIMIT 10;

Структура витрины:
Поле                - Тип данных          - Описание
student_id          - INTEGER             - ID студента
course_id           - INTEGER             - ID курса
department_id       - INTEGER             - Код кафедры
department_name     - VARCHAR             - Название кафедры
education_level     - VARCHAR             - Уровень образования
education_base      - VARCHAR             - Основа обучения
semester            - INTEGER             - Номер семестра
course_year         - INTEGER             - Курс обучения
final_grade         - INTEGER             - Итоговая оценка
total_events        - INTEGER             - Всего событий за семестр
avg_weekly_events   - DECIMAL(10,2)       - Среднее событий в неделю
total_course_views  - INTEGER             - Всего просмотров курса
total_quiz_views    - INTEGER             - Всего просмотров тестов
total_module_views  - INTEGER             - Всего просмотров модулей
total_submissions   - INTEGER             - Всего отправленных заданий
peak_activity_week  - INTEGER             - Неделя с максимальной активностью
consistency_score   - DECIMAL(5,2)        - Коэффициент стабильности активности (0-1)
activity_category   - VARCHAR             - Категория активности (низкая/средняя/высокая)
last_update         - TIMESTAMP           - Дата обновления записи
"""

import os
import sys
from dotenv import load_dotenv
import psycopg2

# Загружаем настройки из файла .env
load_dotenv()

def connect_to_db():
    """Создает подключение к базе данных строго через переменные окружения."""
    try:
        connection = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        connection.autocommit = False
        return connection
    except Exception as error:
        print(f"Ошибка подключения: {error}")
        sys.exit(1)

def init_data_mart_schema(conn):
    """Шаг 1: Подготавливаем схему и пустой каркас витрины."""
    queries = """
    CREATE SCHEMA IF NOT EXISTS dmr;

    CREATE TABLE IF NOT EXISTS dmr.analytics_student_performance (
        student_id         INTEGER NOT NULL,
        course_id          INTEGER NOT NULL,
        department_id      INTEGER,
        department_name    VARCHAR(255),
        education_level    VARCHAR(255),
        education_base     VARCHAR(255),
        semester           INTEGER,
        course_year        INTEGER,
        final_grade        INTEGER,
        total_events       INTEGER,
        avg_weekly_events  DECIMAL(10,2),
        total_course_views INTEGER,
        total_quiz_views   INTEGER,
        total_module_views INTEGER,
        total_submissions  INTEGER,
        peak_activity_week INTEGER,
        consistency_score  DECIMAL(5,2),
        activity_category  VARCHAR(50),
        last_update        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (student_id, course_id)
    );
    """
    with conn.cursor() as cursor:
        cursor.execute(queries)
    conn.commit()
    print("Схема dmr и таблица витрины готовы.")

def load_initial_student_data(conn):
    """Вытягиваем уникальных студентов и их базовые данные."""
    query = """
    INSERT INTO dmr.analytics_student_performance 
        (student_id, course_id, department_id, semester, course_year, final_grade)
    
    SELECT DISTINCT ON (userid, courseid) 
        userid, courseid, Depart, Num_Sem, Kurs, 
        NameR_Level
    FROM public.user_logs
    WHERE NameR_Level IS NOT NULL
    ORDER BY userid, courseid, num_week DESC
    ON CONFLICT (student_id, course_id) DO NOTHING; 
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()
    print("Базовые ID студентов загружены.")

def enrich_categorical_data(conn):
    """Добавляем в витрину текстовую часть (кафедры, форма обучения)."""
    query_deps = """
    UPDATE dmr.analytics_student_performance AS target
    SET department_name = dict.name
    FROM public.departments dict
    WHERE target.department_id = dict.id;
    """

    query_edu = """
    UPDATE dmr.analytics_student_performance AS target
    SET education_level = CASE CAST(source.LevelEd AS VARCHAR) 
        WHEN '1' THEN 'бакалавриат' 
        WHEN '2' THEN 'магистратура' 
        ELSE 'иное' END,
        education_base = CASE CAST(source.Name_OsnO AS VARCHAR) 
        WHEN '1' THEN 'бюджет' 
        WHEN '2' THEN 'контракт' 
        ELSE 'иное' END
    FROM (
        SELECT DISTINCT ON (userid, courseid) userid, courseid, LevelEd, Name_OsnO 
        FROM public.user_logs
    ) source
    WHERE target.student_id = source.userid AND target.course_id = source.courseid;
    """
    with conn.cursor() as cursor:
        cursor.execute(query_deps)
        cursor.execute(query_edu)
    conn.commit()
    print("Категориальные текстовые данные обновлены.")

def calculate_activity_sums(conn):
    """Считаем суммарную активность за весь семестр."""
    query = """
    UPDATE dmr.analytics_student_performance target
    SET total_events = source.t_events,
        total_course_views = source.t_course,
        total_quiz_views = source.t_quiz,
        total_module_views = source.t_module,
        total_submissions = source.t_sub
    FROM (
        SELECT userid, courseid, 
               SUM(s_all) as t_events,
               SUM(s_course_viewed) as t_course,
               SUM(s_q_attempt_viewed) as t_quiz,
               SUM(s_a_course_module_viewed) as t_module,
               SUM(s_a_submission_status_viewed) as t_sub
        FROM public.user_logs
        GROUP BY userid, courseid
    ) as source
    WHERE target.student_id = source.userid AND target.course_id = source.courseid;
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()
    print("Итоговые суммы действий рассчитаны.")

def calculate_advanced_metrics(conn):
    """
    1. Среднее количество событий
    2. Неделя пиковой активности
    3. Коэффициент стабильности
    4. Категория активности
    """
    query_avg_events = """
    UPDATE dmr.analytics_student_performance AS target
    SET avg_weekly_events = ROUND(CAST(target.total_events AS NUMERIC) / NULLIF(source.week_count, 0), 2)
    FROM (
        SELECT userid, courseid, COUNT(DISTINCT num_week) AS week_count 
        FROM public.user_logs 
        GROUP BY userid, courseid
    ) AS source
    WHERE target.student_id = source.userid 
      AND target.course_id = source.courseid;
    """

    query_peak_week = """
    WITH RankedWeeks AS (
        SELECT userid, courseid, num_week, 
               ROW_NUMBER() OVER (PARTITION BY userid, courseid ORDER BY s_all DESC) AS rn 
        FROM public.user_logs
    )
    UPDATE dmr.analytics_student_performance AS target
    SET peak_activity_week = source.num_week
    FROM RankedWeeks AS source
    WHERE source.rn = 1
      AND target.student_id = source.userid 
      AND target.course_id = source.courseid;
    """

    query_consistency = """
    UPDATE dmr.analytics_student_performance AS target
    SET consistency_score = ROUND(CAST(source.act_weeks AS NUMERIC) / NULLIF(source.tot_weeks, 0), 2)
    FROM (
        SELECT userid, courseid, 
               COUNT(DISTINCT num_week) AS tot_weeks, 
               COUNT(DISTINCT CASE WHEN s_all > 0 THEN num_week END) AS act_weeks 
        FROM public.user_logs 
        GROUP BY userid, courseid
    ) AS source
    WHERE target.student_id = source.userid 
      AND target.course_id = source.courseid;
    """

    query_category = """
    WITH ActivityRanks AS (
        SELECT student_id, course_id, 
               PERCENT_RANK() OVER (ORDER BY total_events) AS pr 
        FROM dmr.analytics_student_performance
    )
    UPDATE dmr.analytics_student_performance AS target
    SET activity_category = CASE 
            WHEN source.pr <= 0.25 THEN 'низкая' 
            WHEN source.pr <= 0.75 THEN 'средняя' 
            ELSE 'высокая' 
        END
    FROM ActivityRanks AS source
    WHERE target.student_id = source.student_id 
      AND target.course_id = source.course_id;
    """

    with conn.cursor() as cursor:
        cursor.execute(query_avg_events)
        cursor.execute(query_peak_week)
        cursor.execute(query_consistency)
        cursor.execute(query_category)
    
    conn.commit()
    print("Все продвинутые метрики успешно рассчитаны по шагам.")

def main():
    connection = None
    try:
        print("Подключаемся к базе данных...")
        connection = connect_to_db()
        
        init_data_mart_schema(connection)
        load_initial_student_data(connection)
        enrich_categorical_data(connection)
        calculate_activity_sums(connection)
        calculate_advanced_metrics(connection)
        
        print("\nВсе операции выполнены успешно!")
    except Exception as e:
        print(f"\nОшибка выполнения: {e}")
        if connection:
            connection.rollback()
    finally:
        if connection:
            connection.close()
            print("Соединение с БД закрыто.")

if __name__ == "__main__":
    main()
