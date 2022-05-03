CREATE OR REPLACE VIEW selectel_test.first_view AS 
SELECT
    DATE_TRUNC('MONTH', first_order_date)::DATE AS month,
    SUM(CASE WHEN DATE_TRUNC('MONTH', service_start_date) = DATE_TRUNC(
           'MONTH', first_order_date) THEN 1 ELSE 0 END) AS new_orders
FROM
    (SELECT 
         service_start_date,
         MIN(service_start_date) OVER (PARTITION BY user_id) AS first_order_date
     FROM
         selectel_test.orders) AS transformed
GROUP BY
    DATE_TRUNC('MONTH', first_order_date)
ORDER BY
    month;