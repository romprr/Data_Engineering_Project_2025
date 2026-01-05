DROP TABLE IF EXISTS production.bridge_conflict_side;

CREATE TABLE production.bridge_conflict_side AS (
    SELECT
        e.episode_id,
        ac.actor_id,
        u.side_type AS side,
        u.year
    FROM staging.ucdp_side u
    JOIN production.dim_conflict_episode e ON e.episode_id = u.episode_id
    JOIN production.dim_conflict_actor ac ON ac.actor_id = u.actor_id
);
