"""
py main.py
SELECT * FROM dmr.analytics_student_performance LIMIT 10;

ВИТРИНА ДАННЫХ - ЭТО ОТДЕЛЬНАЯ ТАБЛИЧКА, КОТОРАЯ ПОМОГАЕТ ЛЕГЧЕ АНАЛИЗИРОВАТЬ ДАННЫЕ ПО НУЖНЫМ КАТЕГОРИЯМ

ERCENT_RANK() показывает, какую долю студентов студент обходит (или равен) по активности.
Сортируем всех по total_events от меньшего к большему.
Самый малоактивный получает 0, самый активный — 1.
Например, pr = 0.25 означает, что 25% студентов имеют активность не выше, чем у этого.
То есть это относительное место студента в общем рейтинге активности.

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
# для аварийного выхода из программы 
import sys
# загружает переменные из файла .env в окружение
from dotenv import load_dotenv
#драйвер для подключения к PostgreSQL из Python
import psycopg2

# Загружаем настройки из файла .env
load_dotenv()

def connect_to_db():
    """Создает подключение к базе данных строго через переменные окружения."""
    #устанавливает соединение с БД. Параметры берутся из переменных окружения.
    try:
        connection = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        #отключает автоматический commit. это нужно, чтобы при ошибке можно было откатить всё назад 
        connection.autocommit = False
        return connection
    except Exception as error:
        print(f"Ошибка подключения: {error}")
        sys.exit(1)

def init_data_mart_schema(conn):
    """Шаг 1: Подготавливаем схему и пустой каркас витрины."""
    #многострочная строка в Python.
    #CREATE SCHEMA IF NOT EXISTS dmr — создаёт схему (пространство имён) dmr, если её ещё нет. Это как папка для таблиц.
    #CREATE TABLE IF NOT EXISTS ... — создаёт таблицу витрины. IF NOT EXISTS предотвращает ошибку, если таблица уже существует.
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
    #with conn.cursor() as cursor — автоматически закрывает курсор после выхода из блока.
    with conn.cursor() as cursor:
        ##cursor.execute(queries) — выполняет SQL-команды.
        cursor.execute(queries)
    #conn.commit() — фиксирует изменения в БД (
    conn.commit()
    print("Схема dmr и таблица витрины готовы.")

#Здесь вставляются только основные поля, остальные пока NULL
def load_initial_student_data(conn):
    """Вытягиваем уникальных студентов и их базовые данные."""
    #INSERT INTO dmr.analytics_student_performance  - добавляет новые строки в таблицу и заполняет только указанные столбцы
    #Мы сначала добавляем только «скелет» записи – кто, какой курс, кафедра, семестр, курс обучения, итоговая оценка. 

    #DISTINCT ON – Он берёт строки из таблицы user_logs, группирует их по уникальным парам (userid, courseid), 
    # и из каждой группы выбирает только одну строку, ту, которая идёт первой после сортировки (ORDER BY).
    #Зачем: В таблице user_logs на одного студента и один курс есть много записей (по разным неделям, разным действиям).
    
    #NameR_Level - тут оценки лежат

    #ORDER BY userid, courseid, num_week DESC - сначала сортируем по userid...DESC-по убыванию. Это значит, что для каждого студента 
    # и курса самой верхней строкой будет запись с самым большим номером недели (последняя неделя, где есть оценка).
    #Зачем: Чтобы в витрину попала именно та оценка, которая была на последней неделе (актуальная).
    #Если бы мы не сортировали, могла бы попасть любая случайная неделя.

    #ON CONFLICT (student_id, course_id) DO NOTHING - Если при вставке возникает конфликт – то есть запись с таким же student_id и course_id уже существует в таблице
    #то ничего не делаем и не сохраняем
    #Зачем: Этот скрипт можно запускать несколько раз.
    #Если данные уже есть – мы их не трогаем. А если появились новые студенты или курсы – они добавятся.
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

#SET department_name = dict.name — присваиваем полю department_name значение name из таблицы-справочника.
#WHERE target.department_id = dict.id — соединяем по коду кафедры.
def enrich_categorical_data(conn):
    """Добавляем в витрину текстовую часть (кафедры, форма обучения)."""
    query_deps = """
    UPDATE dmr.analytics_student_performance AS target
    SET department_name = dict.name
    FROM public.departments dict
    WHERE target.department_id = dict.id;
    """
#education_level = ... – присваиваем полю education_level значение, которое вычисляется с помощью CASE.
#CAST(source.LevelEd AS VARCHAR) – приводит значение поля LevelEd к строковому типу.
#WHEN '1' THEN 'бакалавриат' – если значение равно '1', то результат CASE будет 'бакалавриат'

#SELECT DISTINCT ON (userid, courseid) userid, courseid, LevelEd, Name_OsnO – 
#выбирает уникальные пары (userid, courseid) и для каждой пары берёт первую строку 
#source – псевдоним для всего подзапроса
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
        #ursor.execute(query_deps) – выполняет первый запрос (обновление названий кафедр).
        cursor.execute(query_deps)
        cursor.execute(query_edu)
    conn.commit()
    print("Категориальные текстовые данные обновлены.")

#соединение с базой данных - conn
#SET перечислены пары поле_витрины = значение_из_подзапроса
#total_events – поле витрины: общее количество событий за семестр. Присваиваем ему значение source.t_events.
#total_course_views – просмотры курса; берём из source.t_course.
#total_quiz_views – просмотры тестов
#total_module_views – просмотры модулей
#total_submissions – отправленные задания

#SUM(s_all) as t_events – суммируем все события (s_all) за все недели для данной пары студент-курс. Результату даём псевдоним t_events
#Аналогично SUM(s_course_viewed) as t_course – сумма просмотров курса.
#SUM(s_q_attempt_viewed) as t_quiz – сумма просмотров тестов.
#SUM(s_a_course_module_viewed) as t_module – сумма просмотров модулей.
#SUM(s_a_submission_status_viewed) as t_sub – сумма отправленных заданий.
#FROM public.user_logs – данные берём из таблицы логов.
#GROUP BY userid, courseid – группируем строки по студенту и курсу
#WHERE target.student_id = source.userid AND target.course_id = source.courseid - для каждой строки витрины находим соответствующую строку в подзапросе с 
#теми же студентом и курсом и берём оттуда суммы для обновления.
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

#
def calculate_advanced_metrics(conn):
    """
    1. Среднее количество событий в неделю
    2. Неделя пиковой активности
    3. Коэффициент стабильности
    4. Категория активности
    """
    #SELECT userid, courseid, COUNT(DISTINCT num_week) AS week_count – для каждого студента и курса считает количество уникальных недель
    #COUNT(DISTINCT num_week) игнорирует дубликаты номеров недель.
    #GROUP BY userid, courseid – группировка по паре студент-курс.

    #CAST(target.total_events AS NUMERIC) – превращает целое число событий в тип NUMERIC (с плавающей точкой)
    #NULLIF(source.week_count, 0) – если week_count равно 0, возвращает NULL (чтобы избежать деления на ноль).
    #WHERE target.student_id = source.userid AND target.course_id = source.courseid – соединяем витрину с подзапросом по ключу.
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

    #WITH создаёт временную именованную таблицу RankedWeeks
    # ROW_NUMBER() – это оконная функция, которая присваивает каждой строке уникальный номер в пределах группы (окна). Нумерация начинается с 1.
    # ORDER BY s_all DESC – внутри каждой группы строки сортируются по убыванию поля s_all
    # PARTITION BY userid, courseid – разбивает все строки на группы по студенту и курсу. Внутри каждой группы нумерация начинается заново.
    
    #SET peak_activity_week = source.num_week – поле peak_activity_week устанавливаем равным num_week из CTE.
    #WHERE source.rn = 1 – берём только те строки из CTE, у которых rn = 1, то есть самую активную неделю для каждой пары студент-курс.
    #AND target.student_id = source.userid AND target.course_id = source.courseid – соединяем строки витрины с CTE по ключу. 
    #Для каждого студента и курса в витрине находим соответствующую самую активную неделю
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
    #COUNT(DISTINCT num_week) – считает уникальные значения num_week для данного студента и курса.
    #сколько всего недель студент хоть как-то фигурировал в логах (например, заходил на платформу, но ничего не делал).
    #CASE WHEN s_all > 0 THEN num_week END – проверяет: если количество событий за неделю больше нуля, то возвращает num_week; иначе возвращает NULL.
    #Результат: количество уникальных недель, в которых была хоть какая-то ненулевая активность.

    #Присваиваем полю consistency_score вычисленное значение.
    #CAST(source.act_weeks AS NUMERIC): Преобразуем act_weeks (целое число) 
    #NULLIF(source.tot_weeks, 0): если tot_weeks = 0 (у студента нет ни одной недели), то вместо 0 подставится NULL, чтобы не делить на 0, останется null
    # ЭТО В СЛУЧАЕ, ЕСЛИ ВСЕ ЗАПИСИ 0, НО ЗАПИСИ ЕСТЬ. ЕСЛИ ВООБЩЕ НЕТ - NULL.
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
    #Разбиение происходит по принципу сравнения студенты с разных курсов. Если нужно сравнивать только внутри одного курса, следовало бы добавить PARTITION BY course_id.
    #PERCENT_RANK() - вычисляем относительное место студента в общем рейтинге (позиция строки в стортировке - 1)/(общее кол-во строк в окне - 1)
    #ORDER BY total_events – сортирует все строки витрины по возрастанию общего числа событий (от самых малоактивных до самых активных).
    
    #FROM ActivityRanks AS source – присоединяем CTE (временную таблицу) под псевдонимом source.
    #WHERE target.student_id = source.student_id AND target.course_id = source.course_id – соединяем витрину с CTE по первичному ключу. 
    #Для каждой строки витрины находим соответствующую строку в source с уже вычисленным pr.
    #
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
        #выполнение запроса
        cursor.execute(query_avg_events)
        cursor.execute(query_peak_week)
        cursor.execute(query_consistency)
        cursor.execute(query_category)
    #фиксация
    conn.commit()
    print("Все метрики успешно рассчитаны по шагам.")

def main():
    connection = None
    try:
        print("Подключаемся к базе данных...")
        #Подключение к БД
        connection = connect_to_db()
        
        #создаёт схему dmr и таблицу витрины, если их нет
        init_data_mart_schema(connection)
        # вставляет базовые записи (студент, курс, оценка и т.д.).
        load_initial_student_data(connection)
        #добавляет текстовые названия кафедр, уровня образования, основы.
        enrich_categorical_data(connection)
        #считает суммарные показатели активности.
        calculate_activity_sums(connection)
        #вычисляет среднее, пиковую неделю, стабильность, категорию.
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
