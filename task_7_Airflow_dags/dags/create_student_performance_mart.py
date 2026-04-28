# импортируем классы для работы с датой и временем. datetime используется для 
# указания даты начала (start_date), а timedelta – для задания интервалов 
# (например, задержки перед повторной попыткой).
from datetime import datetime, timedelta
# импортируем основной класс DAG. Объект этого класса будет описывать наш граф задач (набор связанных операций).
from airflow import DAG
#импортируем оператор для выполнения SQL-кода в PostgreSQL. Этот оператор принимает идентификатор подключения 
# (conn_id) и SQL-запрос (строку или путь к файлу), после чего выполняет его в указанной базе данных.
from airflow.providers.postgres.operators.postgres import PostgresOperator

#default_args – словарь с параметрами по умолчанию, которые будут применены ко всем задачам внутри DAG. 
# Это позволяет не дублировать настройки для каждой задачи.
default_args = {
    'owner': 'airflow', # владелец DAG
    'depends_on_past': False, #означает, что успех текущего запуска не зависит от успеха предыдущего запуска того же DAG. 
    #Если True, то задача будет ждать успешного выполнения предыдущего запуска (по расписанию).
    'start_date': datetime(2026, 4, 28), #дата, с которой DAG начинает работу.
    'email_on_failure': False, #не отправлять письмо при ошибке задачи.
    'email_on_retry': False, # не отправлять письмо при повторной попытке.
    'retries': 1, # количество повторных попыток при падении задачи.
    'retry_delay': timedelta(minutes=1), #задержка между повторными попытками (1 минута).
}

sql_full = """
DROP SCHEMA IF EXISTS dmr CASCADE;
CREATE SCHEMA dmr;

CREATE TABLE dmr.analytics_student_performance (
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

INSERT INTO dmr.analytics_student_performance 
    (student_id, course_id, department_id, semester, course_year, final_grade)
SELECT DISTINCT ON (userid, courseid) 
    userid, courseid, Depart, Num_Sem, Kurs, NameR_Level
FROM public.user_logs
WHERE NameR_Level IS NOT NULL
ORDER BY userid, courseid, num_week DESC
ON CONFLICT (student_id, course_id) DO NOTHING;

UPDATE dmr.analytics_student_performance AS target
SET department_name = dict.name
FROM public.departments dict
WHERE target.department_id = dict.id;

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

UPDATE dmr.analytics_student_performance AS target
SET avg_weekly_events = ROUND(CAST(target.total_events AS NUMERIC) / NULLIF(source.week_count, 0), 2)
FROM (
    SELECT userid, courseid, COUNT(DISTINCT num_week) AS week_count 
    FROM public.user_logs 
    GROUP BY userid, courseid
) AS source
WHERE target.student_id = source.userid 
  AND target.course_id = source.courseid;

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
#конструкция контекстного менеджера. 
# Все операторы, созданные внутри блока with, автоматически привязываются к этому DAG.
with DAG(
    dag_id='create_student_performance_mart', #уникальный идентификатор DAG в системе Airflow. Он будет отображаться в веб-интерфейсе.
    default_args=default_args, #передаём словарь с параметрами по умолчанию.
    description='Создаёт витрину dmr.analytics_student_performance', # текстовое описание DAG, видно в UI.
    schedule_interval=None, #означает, что DAG не будет запускаться автоматически по расписанию. 
    #Его можно запускать только вручную (кнопка «Trigger DAG»).
    catchup=False, #запрещает «догонять» пропущенные интервалы. Поскольку расписания нет, параметр не принципиален, но обычно ставят False.
    tags=['student', 'mart'], #метки для группировки DAG в интерфейсе. Позволяют фильтровать.
) as dag:
    create_mart = PostgresOperator( #создаём экземпляр оператора и присваиваем его переменной create_mart. 
        #В Airflow этот объект будет представлять задачу (task). Переменная нужна только для определения зависимостей, но если в DAG одна задача, 
        #её можно не использовать (но обычно присваивают для наглядности).
        task_id='create_mart', #уникальное имя задачи внутри DAG. Оно отображается в UI и используется для ссылок при установке порядка выполнения.
        postgres_conn_id='postgres_default', #– идентификатор соединения, которое заранее настраивается в Airflow (Admin → Connections).
        sql=sql_full, #SQL-код, который должен быть выполнен. 
    )