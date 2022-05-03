CREATE OR REPLACE VIEW selectel_test.second_view AS
SELECT
    user_id,
    COUNT(id) FILTER(WHERE DATE_TRUNC('MONTH', service_start_date)
    = DATE_TRUNC('MONTH', first_order_date)) AS month1,
    COUNT(id) FILTER(WHERE DATE_TRUNC('MONTH', service_start_date) 
    = DATE_TRUNC('MONTH', first_order_date) + INTERVAL '1 MONTHS') AS month2,
    COUNT(id) FILTER(WHERE DATE_TRUNC('MONTH', service_start_date)
    = DATE_TRUNC('MONTH', first_order_date) + INTERVAL '2 MONTHS') AS month3
FROM
    (SELECT
         user_id,
         id,
         service_start_date,
         MIN(service_start_date) OVER (PARTITION BY user_id) AS first_order_date
     FROM
         selectel_test.orders) AS transformed
GROUP BY 
    user_id;