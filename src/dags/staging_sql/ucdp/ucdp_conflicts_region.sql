DROP TABLE IF EXISTS ucdp_conflict_region;

CREATE TABLE ucdp_conflict_region AS (
    SELECT DISTINCT
        conflict_id as conflict_id,
        TRIM(region_split)::INT AS region_id
    FROM CONFLICT,
        LATERAL regexp_split_to_table(region, ',') AS region_split
    ORDER BY conflict_id, TRIM(region_split)::INT
)