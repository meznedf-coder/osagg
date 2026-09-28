SELECT "NODE" AS "NODE", sum("JOB_DURATION_d") AS total_duration, AVG("JOB_DURATION_d") AS avg_duration, max("JOB_DURATION_d") AS max_duration, COUNT(*) AS count 
FROM "default"."jobs" 
WHERE "@timestamp_date" >= TIMESTAMP '2026-03-25 00:00:00.000000' AND "@timestamp_date" < TIMESTAMP '2026-04-02 00:00:00.000000' GROUP BY "NODE" ORDER BY max_duration DESC 
 LIMIT 15;