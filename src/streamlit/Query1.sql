SELECT DISTINCT
    ce.episode_id,
	av.asset_id,
	ce.episode_start,

    AVG(CASE
        WHEN m.month_date < ce.episode_start THEN av.close
        ELSE NULL END) as avg_before_episode,
    AVG(CASE
        WHEN m.month_date > ce.episode_start THEN av.close
        ELSE NULL END) as avg_after_episode,
	AVG(av.close) as avg_all,

	to_char(
		(AVG(CASE
        WHEN m.month_date > ce.episode_start THEN av.close
        ELSE NULL END) - AVG(CASE
        WHEN m.month_date < ce.episode_start THEN av.close
        ELSE NULL END)) / 100,
		'FMSG9999990.0') || '%' as t


FROM production.fact_conflict_episode ce
-- Gets all the date before and after 3 months of the start of an episode
-- This time no need to use the bridge as we look at a range and not a specific id
JOIN production.dim_month_date m ON
    m.month_date BETWEEN 
        DATE_TRUNC('month', ce.episode_start) - INTERVAL '3 months'
        AND DATE_TRUNC('month', ce.episode_start) + INTERVAL '3 months'
JOIN production.dim_region r ON r.region_id = ce.region_id
JOIN production.fact_asset_value av ON m.month_id = av.date_id AND av.region_id = r.region_id

GROUP BY ce.episode_id,
	av.asset_id,
	ce.episode_start

ORDER BY ce.episode_start