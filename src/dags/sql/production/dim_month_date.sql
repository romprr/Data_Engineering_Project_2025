DROP TABLE IF EXISTS production.dim_month_date;

CREATE TABLE production.dim_month_date AS (
    WITH start_date_cte AS (
        SELECT MIN(t) as first_ts 
        FROM (
            SELECT "date" as t FROM staging.index_value
            UNION ALL
            SELECT "date" as t FROM staging.forex_value
            UNION ALL
            SELECT "date" as t FROM staging.future_value
            UNION ALL
            SELECT "date" as t FROM staging.crypto_value
        ) subquery
    )
    SELECT 
        d::date AS month_date,
        to_char(d, 'YYYYMM')::int AS month_id,
        TRIM(to_char(d, 'Month')) AS month_name,
        EXTRACT(year FROM d)::int AS year,
        EXTRACT(quarter FROM d)::int AS quarter
    FROM generate_series(
        (SELECT DATE_TRUNC('month', first_ts - INTERVAL '1 month') FROM start_date_cte)::date,  -- Start Date
        DATE_TRUNC('month', CURRENT_DATE)::date,  -- End Date
        '1 month'            -- Interval
    ) AS d;
)