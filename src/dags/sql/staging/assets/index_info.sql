DROP TABLE IF EXISTS  staging.index_info;

CREATE TABLE staging.index_info AS (
    SELECT
        'index_' || i.symbol || '_asset' AS index_id,
        i.symbol,
        i.currency,
        i.region,
        i.index_name,
        i.exchange_timezone
    FROM raw.INDEX_EXCHANGE i
    WHERE LENGTH(REPLACE(i.currency, ' ', '')) > 0
);