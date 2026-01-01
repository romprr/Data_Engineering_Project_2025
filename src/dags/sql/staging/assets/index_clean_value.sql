DROP TABLE IF EXISTS staging.index_clean_value;

CREATE TABLE staging.index_clean_value AS (
    WITH normalized_value AS (
        SELECT index_id, date, value_open, value_high, value_low, value_close, volume, dividends, region,
            (CASE
                WHEN EXTRACT('Day' FROM date) != 1 THEN date + INTERVAL '1 day'
                ELSE date
            END)::DATE as clean_date
        FROM staging.index_value
        ORDER BY date
    ),
    RankedNormalizedValue AS (
        SELECT *,
        ROW_NUMBER() OVER(
            PARTITION BY index_id, clean_date
            ORDER BY date DESC
        ) AS nv
        FROM normalized_value
    )

    SELECT index_id, clean_date, value_open, value_high, value_low, value_close, volume, dividends, region
    FROM RankedNormalizedValue
    WHERE nv = 1
)