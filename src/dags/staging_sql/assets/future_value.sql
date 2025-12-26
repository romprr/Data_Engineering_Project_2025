DROP TABLE IF EXISTS future_value;

CREATE TABLE future_value AS (
    SELECT
        'future_' || fv.symbol || '_value' AS future_id,
        fv.value_timestamp AS "timestamp",
        fv.value_open,
        fv.value_high,
        fv.value_low,
        fv.value_close,
        fv.volume
    FROM FUTURES_HISTORY fv
);