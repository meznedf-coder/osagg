SELECT DATE_TRUNC('week', "@timestamp_date" + INTERVAL '1 day') - INTERVAL '1 day' AS "@timestamp_date", COUNT(*) AS count 
FROM "default"."jobs" 
WHERE "@timestamp_date" >= TIMESTAMP '2026-03-25 00:00:00.000000' AND "@timestamp_date" < TIMESTAMP '2026-04-02 00:00:00.000000' GROUP BY DATE_TRUNC('week', "@timestamp_date" + INTERVAL '1 day') - INTERVAL '1 day' 
 LIMIT 1000;