SELECT
	CASE
		WHEN fcem.month_id IS NOT NULL THEN '1. Fresh Conflict Month (New War started)'
		ELSE '2. Status Quo Month (Ongoing Wars or Peace)'
	END AS global_news_state,
	COUNT(DISTINCT d.month_id) AS months_count,
	ROUND(AVG((fav.close - fav.open) / fav.open) * 100, 3) as average_return_pct
	

FROM production.fact_asset_value fav
	JOIN production.dim_asset_info ai 
		ON fav.asset_id = ai.asset_id AND ai.name = 'S&P 500'
    JOIN production.dim_month_date d 
		ON fav.date_id = d.month_id
	LEFT JOIN production.fact_conflict_episode_monthly fcem 
		ON fcem.month_id = d.month_id AND fcem.episode_status = 'Started'
GROUP BY 1
ORDER BY 1