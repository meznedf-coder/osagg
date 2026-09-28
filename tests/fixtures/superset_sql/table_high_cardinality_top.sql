SELECT "PARAM_ID" AS "PARAM_ID", sum("JOB_DURATION_d") AS total_duration 
FROM "default"."jobs" 
WHERE "@timestamp_date" >= TIMESTAMP '2026-03-25 00:00:00.000000' AND "@timestamp_date" < TIMESTAMP '2026-04-02 00:00:00.000000' GROUP BY "PARAM_ID" ORDER BY total_duration DESC 
 LIMIT 20;