
UPDATE user_logs SET s_all_avg = REPLACE(s_all_avg, ',', '.') WHERE s_all_avg LIKE '%,%';

ALTER TABLE user_logs ALTER COLUMN s_all_avg TYPE REAL USING s_all_avg::REAL;

SELECT AVG(s_all_avg) AS average_activity FROM user_logs;