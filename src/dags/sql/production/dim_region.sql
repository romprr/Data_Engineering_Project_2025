DROP TABLE IF EXISTS production.dim_region;

CREATE TABLE production.dim_region AS (
    SELECT * FROM (
        VALUES
            (1, 'Europe'),
            (2, 'Middle East'),
            (3, 'Asia'),
            (4, 'Africa'),
            (5, 'Americas'),
            (6, 'Unknown')
    ) AS dim_region(region_id, region)
)