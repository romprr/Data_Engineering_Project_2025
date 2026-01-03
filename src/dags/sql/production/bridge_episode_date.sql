-- DROP TABLE IF EXISTS production.bridge_episode_date;

-- CREATE TABLE production.bridge_episode_date AS (
--     SELECT
--         e.episode_id,
--         d.month_id
--     FROM staging.ucdp_episode e
--     JOIN production.dim_month_date d ON 
--         d.month_date = DATE_TRUNC('month', e.episode_start) OR
--         (d.month_date > DATE_TRUNC('month', e.episode_start) AND 
--         d.month_date < DATE_TRUNC('month', COALESCE(e.episode_end, DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '1 month'))) OR
--         d.month_date = DATE_TRUNC('month', COALESCE(e.episode_end, DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '1 month'))
--     -- Here we want to bridge the episodes to the months
--     -- If the end date is unknown, it means that we don't know if the conflict ended
--     -- So we set the null to last year's last month (e.g 2024-12-01 if we are in 2025)
-- )

-- [ARCHIVED QUERY]