DROP TABLE IF EXISTS  staging.ucdp_conflict;

CREATE TABLE staging.ucdp_conflict AS (
    SELECT DISTINCT
        c.conflict_id as conflict_id,
        c.incompatibility as reason,
        MAX(c.type_of_conflict) as conflict_type,
        c.territory_name as disputed_territory,
        c.start_date as "start_date",
        ARRAY(
            SELECT TRIM(x)
            FROM UNNEST(STRING_TO_ARRAY(c.location, ',')) as x
            ) as locations,
        ARRAY(
            SELECT CASE x 
                WHEN 1 THEN 'Europe'
                WHEN 2 THEN 'Middle East'
                WHEN 3 THEN 'Asia'
                WHEN 4 THEN 'Africa'
                WHEN 5 THEN 'Americas'
            END
            FROM UNNEST(STRING_TO_ARRAY(c.region, ',')::INT[]) as x
        ) as regions
    FROM
        raw.CONFLICT c
    GROUP BY c.conflict_id, c.incompatibility, c.territory_name, c.start_date, c.location, c.region
);