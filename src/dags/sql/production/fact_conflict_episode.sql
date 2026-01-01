DROP TABLE IF EXISTS production.fact_conflict_episode;

CREATE TABLE production.fact_conflict_episode AS (
    SELECT
        e.*,
        CASE
            WHEN ug.conflict_id IS NULL THEN cs_ins.region_id
            ELSE cs_geo.region_id
        END AS region_id,

    FROM staging.ucdp_episode e
    JOIN production.dim_conflict_info c
        ON c.conflict_id = e.conflict_id
    
    LEFT JOIN staging.ucdp_georeference ug ON ug.conflict_id = c.conflict_id
    LEFT JOIN production.dim_conflict_side cs_geo ON csg_geo.region = ug.region
    LEFT JOIN production.dim_conflict_side cs_ins ON cs_ins.region = TRIM(c.regions[1]) --fallback
)