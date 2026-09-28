SELECT DATE_TRUNC('day', "@timestamp_date") AS "@timestamp_date", COUNT(*) AS count 
FROM "default"."jobs" 
WHERE "@timestamp_date" >= TIMESTAMP '2026-03-25 00:00:00.000000' AND "@timestamp_date" < TIMESTAMP '2026-04-02 00:00:00.000000' GROUP BY DATE_TRUNC('day', "@timestamp_date") 
 LIMIT 10000;