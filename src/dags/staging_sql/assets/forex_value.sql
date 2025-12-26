DROP TABLE IF EXISTS forex_value;

CREATE TABLE forex_value AS (
    SELECT
        'forex_' || fv.symbol || '_value' AS forex_id,
        fv.value_timestamp AS "timestamp",
        fv.value_open,
        fv.value_high,
        fv.value_low,
        fv.value_close
    FROM FOREX_HISTORY fv
);