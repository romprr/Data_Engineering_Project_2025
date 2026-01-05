DROP TABLE IF EXISTS production.fact_conflict_episode_monthly;

CREATE TABLE production.fact_conflict_episode_monthly AS (
    SELECT
        d.month_id,
        e.episode_id,
        e.conflict_id,
        CASE
            WHEN ug.conflict_id IS NULL THEN cs_ins.region_id
            ELSE cs_geo.region_id
        END AS region_id,

        CASE
            WHEN d.month_date = DATE_TRUNC('month', e.episode_start) THEN 'Started'
            WHEN d.month_date = DATE_TRUNC('month', e.episode_end) THEN 'Ended'
            ELSE 'Ongoing'
        END AS episode_status,

		(
		CASE
			WHEN e.episode_end < (d.month_date + interval '1 month' - interval '1 day')::DATE
				THEN e.episode_end
			ELSE (d.month_date + interval '1 month' - interval '1 day')::DATE
		END
		-
		CASE
			WHEN e.episode_start > d.month_date THEN e.episode_start
			ELSE d.month_date
		END
		) + 1 AS monthly_days_active


    FROM production.dim_conflict_episode e
    JOIN production.dim_conflict_info c
        ON c.conflict_id = e.conflict_id

    JOIN production.dim_month_date d ON 
        d.month_date = DATE_TRUNC('month', e.episode_start) OR
        (d.month_date > DATE_TRUNC('month', e.episode_start) AND 
        d.month_date < DATE_TRUNC('month', COALESCE(e.episode_end, DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '1 month'))) OR
        d.month_date = DATE_TRUNC('month', COALESCE(e.episode_end, DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '1 month'))
    
    LEFT JOIN staging.ucdp_georeference ug ON ug.conflict_id = c.conflict_id
    LEFT JOIN production.dim_region cs_geo ON cs_geo.region = ug.region
    LEFT JOIN production.dim_region cs_ins ON cs_ins.region = TRIM(c.primary_parties_regions[1]) --fallback
)