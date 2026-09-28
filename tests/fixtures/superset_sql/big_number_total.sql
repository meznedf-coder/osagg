SELECT SUM(CASE WHEN "STATUS_INFO" = 'FAILED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS failure_rate_pct 
FROM "default"."jobs" 
WHERE "@timestamp_date" >= TIMESTAMP '2026-03-25 00:00:00.000000' AND "@timestamp_date" < TIMESTAMP '2026-04-02 00:00:00.000000' 
 LIMIT 1;