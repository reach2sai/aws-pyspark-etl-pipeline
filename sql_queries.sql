-- Register the output Parquet directory as a Spark SQL view before running:
-- CREATE OR REPLACE TEMP VIEW shipments USING parquet OPTIONS (path 'output/shipments');

SELECT carrier, COUNT(*) AS total_shipments,
       SUM(CASE WHEN on_time = true THEN 1 ELSE 0 END) AS on_time_deliveries
FROM shipments
GROUP BY carrier
ORDER BY total_shipments DESC;

SELECT status, COUNT(*) AS shipment_count
FROM shipments
GROUP BY status;
