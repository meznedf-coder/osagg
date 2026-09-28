SELECT TIME_BUCKET(INTERVAL '10 minutes', "@timestamp_date") AS "@timestamp_date", sum("CPU_COST_H_d") AS total_cpu_cost 
FROM "default"."jobs" 
WHERE "@timestamp_date" >= TIMESTAMP '2026-03-25 00:00:00.000000' AND "@timestamp_date" < TIMESTAMP '2026-04-02 00:00:00.000000' AND "APPLICATION" IN ('BILLING', 'ORDERS') GROUP BY TIME_BUCKET(INTERVAL '10 minutes', "@timestamp_date") 
 LIMIT 50000;