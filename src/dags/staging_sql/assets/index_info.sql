DROP TABLE IF EXISTS index_info;

CREATE TABLE index_info AS (
    SELECT
        'index_' || i.symbol || '_info' AS index_id,
        i.symbol,
        i.currency,
        i.region,
        i.index_name,
        i.exchange_timezone
    FROM INDEX_EXCHANGE i
);