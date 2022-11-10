with user_transactions AS (
  SELECT 896 as user_id, 12568001 as transaction_id, 1001 as product_id, DATE('2021-03-02') as payment_date, 19.99 as total_cost UNION ALL
  SELECT 896, 12568002, 1002, DATE('2021-03-02'), 29.99 UNION ALL
  SELECT 896, 12568003, 1003, DATE('2021-03-02'), 39.99 UNION ALL
  SELECT 896, 12568004, 2001, DATE('2021-03-02'), 199.99 UNION ALL
  SELECT 896, 12568005, 8881, DATE('2021-03-03'), 4.99 UNION ALL
  SELECT 896, 12568006, 9020, DATE('2021-03-03'), 8.99 UNION ALL
  SELECT 896, 12578004, 3040, DATE('2021-03-03'), 34.99 UNION ALL
  SELECT 896, 12578005, 3041, DATE('2021-03-08'), 34.99),
paypal_transactions AS (
  SELECT 
    *
  FROM 
  UNNEST(ARRAY<STRUCT<dt STRING, type STRING, status STRING, currency STRING, gross FLOAT64, itemid INT64, balanceimpact STRING>>[
      ('07/03/2021', 'Website Payment', 'Completed', 'USD', 19.99, 12568001, 'Credit'),
      ('03/03/2021', 'Website Payment', 'Completed', 'USD', 29.99, 12568002, 'Credit'),
      ('03/03/2021', 'Website Payment', 'Completed', 'USD', 39.99, 12568003, 'Credit'),
      ('03/03/2021', 'Website Payment', 'Completed', 'USD', 14.99, 12568004, 'Credit'),
      ('05/03/2021', 'Website Payment', 'Completed', 'USD', 42.99, 12568005, 'Credit'),
      ('05/03/2021', 'Website Payment', 'Completed', 'USD', 80.99, 12568006, 'Credit'),
      ('05/03/2021', 'Website Payment', 'Completed', 'USD', 34.99, 12578004, 'Credit'),
      ('04/03/2021', 'Website Payment', 'Completed', 'USD', 34.99, 12578005, 'Credit')
    ]
  )
)

SELECT
  p.dt,
  p.status,
  SUM(p.gross) AS confirmed_revenue_usd
FROM user_transactions u
INNER JOIN paypal_transactions p 
  ON u.transaction_id = p.itemid
GROUP BY
  p.dt, p.status
;
