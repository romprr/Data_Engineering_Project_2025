DROP TABLE IF EXISTS staging.forex_clean_value;

CREATE TABLE staging.forex_clean_value AS (
    WITH normalized_value AS (
        SELECT forex_id, date, value_open, value_high, value_low, value_close, region,
            (CASE
                WHEN EXTRACT('Day' FROM date) != 1 THEN date + INTERVAL '1 day'
                ELSE date
            END)::DATE as clean_date
        FROM staging.forex_value
        ORDER BY date
    ),
    RankedNormalizedValue AS (
        SELECT *,
        ROW_NUMBER() OVER(
            PARTITION BY forex_id, clean_date
            ORDER BY date DESC
        ) AS nv
        FROM normalized_value
    )

    SELECT forex_id, clean_date, value_open, value_high, value_low, value_close, region
    FROM RankedNormalizedValue
    WHERE nv = 1
)