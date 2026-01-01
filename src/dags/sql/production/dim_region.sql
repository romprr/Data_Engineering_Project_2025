SELECT * FROM (
    VALUES
        (1, 'Europe'),
        (2, 'Middle East'),
        (3, 'Asia'),
        (4, 'Africa'),
        (5, 'Americas')
) AS dim_region(region_id, region);