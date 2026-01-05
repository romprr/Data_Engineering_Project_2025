DROP TABLE IF EXISTS staging.ucdp_georeference;

CREATE TABLE staging.ucdp_georeference AS (
    WITH geo_grouped AS (
        SELECT
            gr.conflict_new_id AS conflict_id,
            gr.country,
            gr.region,
            COUNT(gr.country) as number_of_appearance
        FROM raw.ucdp_georeference gr
        JOIN staging.ucdp_conflict uc ON uc.conflict_id = gr.conflict_new_id
        GROUP BY gr.conflict_new_id, gr.country, gr.region
    )
    SELECT DISTINCT ON (conflict_id)
        conflict_id,
        country,
        region
    FROM geo_grouped
    ORDER BY
        conflict_id,
        number_of_appearance DESC
);