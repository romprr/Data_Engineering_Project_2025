DROP TABLE IF EXISTS production.fact_conflict_episode;

CREATE TABLE production.fact_conflict_episode AS (
    SELECT * FROM staging.ucdp_episode
)