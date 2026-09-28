SELECT "@timestamp_date" AS "@timestamp_date", "RUN_ID" AS "RUN_ID", "APPLICATION" AS "APPLICATION", "JOB_DURATION_d" AS "JOB_DURATION_d" 
FROM "default"."jobs" 
WHERE "@timestamp_date" >= TIMESTAMP '2026-03-25 00:00:00.000000' AND "@timestamp_date" < TIMESTAMP '2026-04-02 00:00:00.000000' AND "STATUS_INFO" = 'KILLED' ORDER BY "@timestamp_date" DESC, "RUN_ID" ASC 
 LIMIT 50;