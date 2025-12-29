DROP TABLE IF EXISTS production.dim_conflit_infos;

CREATE TABLE production.dim_conflict_info AS (
    SELECT * FROM staging.ucdp_conflict
)