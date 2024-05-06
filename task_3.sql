CREATE TABLE task_3.transactions (
  id int64,
  subscription_billing_day int64,
  start_date date,
  end_date date,
  duration_month float64
);

INSERT INTO task_3.transactions (id,subscription_billing_day, start_date, end_date, duration_month)
VALUES
  (1, 29, '2024-01-29', '2024-05-29', 4),
  (2, 12, '2024-01-12', '2024-07-12', 6);

CREATE OR REPLACE TABLE task_3.rr_transactions AS (
  with recursive cte as (
    select 
      id, 
      start_date, 
      DATE(start_date + interval 1 month) as end_date 
    from task_3.transactions
    union all
    select 
      cte.id,
      DATE(cte.start_date + interval 1 month),
      DATE(cte.end_date + interval 1 month)
    from cte join task_3.transactions t using(id)
    where cte.end_date < t.end_date
  )
  select 
    id,
    start_date as rr_start_date,
    end_date as rr_end_date,
    CAST(timestamp_diff(end_date, start_date, month) as float64) as duration_rr
  from cte
  order by id, start_date
);