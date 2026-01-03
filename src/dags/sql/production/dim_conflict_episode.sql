DROP TABLE IF EXISTS production.dim_conflict_episode;

CREATE TABLE production.dim_conflict_episode AS (
    SELECT
        e.*
    FROM staging.ucdp_episode e
    JOIN production.dim_conflict_info c
        ON c.conflict_id = e.conflict_id
)