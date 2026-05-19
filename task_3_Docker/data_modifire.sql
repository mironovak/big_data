
UPDATE user_logs SET s_all_avg = REPLACE(s_all_avg, ',', '.') WHERE s_all_avg LIKE '%,%';

ALTER TABLE user_logs ALTER COLUMN s_all_avg TYPE REAL USING s_all_avg::REAL;

SELECT AVG(s_all_avg) AS average_activity FROM user_logs;



LTER TABLE public.user_logs ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.user_logs ALTER COLUMN namer_level TYPE int USING namer_level::int;
ALTER TABLE public.user_logs ALTER COLUMN date_vatt TYPE varchar(255) USING date_vatt::varchar(255);

ALTER TABLE public.user_logs ALTER COLUMN s_q_attempt_viewed_avg TYPE int4 USING s_q_attempt_viewed_avg::int4;



select d."name",  COUNT(DISTINCT u.courseid) as course_count
FROM user_logs u
JOIN departments d ON u.depart = d.id
GROUP BY d.name
ORDER BY course_count desc
