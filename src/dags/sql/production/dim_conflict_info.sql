DROP TABLE IF EXISTS production.dim_conflict_info;

CREATE TABLE production.dim_conflict_info AS (
    SELECT 
        uc.conflict_id,
        uc.reason,
        uc.conflict_type,
        uc.disputed_territory,
        uc.start_date,
        uc.locations as primary_parties_locations,
        uc.regions as primary_parties_regions
    FROM staging.ucdp_conflict uc

)