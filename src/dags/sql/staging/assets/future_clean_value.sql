DROP TABLE IF EXISTS staging.future_clean_value;

CREATE TABLE staging.future_clean_value AS (
    WITH normalized_value AS (
        SELECT future_id, date, value_open, value_high, value_low, value_close, volume, region,
            (CASE
                WHEN EXTRACT('Day' FROM date) != 1 THEN date + INTERVAL '1 day'
                ELSE date
            END)::DATE as clean_date
        FROM staging.future_value
        ORDER BY date
    ),
    RankedNormalizedValue AS (
        SELECT *,
        ROW_NUMBER() OVER(
            PARTITION BY future_id, clean_date
            ORDER BY date DESC
        ) AS nv
        FROM normalized_value
    )

    SELECT future_id, clean_date, value_open, value_high, value_low, value_close, volume, region
    FROM RankedNormalizedValue
    WHERE nv = 1
)