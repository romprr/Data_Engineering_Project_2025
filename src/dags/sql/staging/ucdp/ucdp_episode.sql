DROP TABLE IF EXISTS  staging.ucdp_episode;

CREATE TABLE staging.ucdp_episode AS (
    SELECT
        c.conflict_id || '_' || c.start_date2 AS episode_id,
        c.conflict_id AS conflict_id,
        MAX(c.type_of_conflict) as conflict_type,
        c.start_date2 as episode_start,
        MAX(c.ep_end_date) as episode_end,
        MAX(c.cumulative_intensity) as cumulative_intensity
    FROM
        raw.CONFLICT c
    GROUP BY
        c.conflict_id,
        c.start_date2
);


