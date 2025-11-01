/*
5. **Incremental query for `actors_history_scd`:** 
Write an "incremental" query that combines the previous year's SCD 
data with new incoming data from the `actors` table.
*/
CREATE TYPE scd_type as (
	quality_class quality_class,
	is_active boolean,
	start_date INTEGER,
	end_date INTEGER
)
WITH last_year_scd AS (
	SELECT * FROM actors_history_scd
	WHERE current_year = 2021
	AND end_date = 1980
),
historical_scd AS (
	SELECT
		actorname,
		quality_class,
		is_active,
		start_date,
		end_date
	FROM actors_history_scd
	WHERE current_year = 2021
	AND end_date < 1980
),
this_year_data AS (
	SELECT *
	FROM actors
	WHERE current_year = 2022
),
unchanged_records AS (
	SELECT
		ty.actorname,
		ty.quality_class,
		ty.is_active,
		ly.start_date,
		ty.current_year as end_date
	FROM this_year_data ty
	JOIN last_year_scd ly ON ly.actorid = ty.actorid
	WHERE ty.quality_class = ly.quality_class
	AND ty.is_active = ly.is_active
),
changed_records AS (
	SELECT
		ty.actorname,
		UNNEST(ARRAY[
            ROW(
                ly.quality_class,
                ly.is_active,
                ly.start_date,
                ly.end_date

            )::scd_type,
            ROW(
                ty.quality_class,
                ty.is_active,
                ty.current_year,
                ty.current_year
            )::scd_type
        ]) AS records
	FROM this_year_data ty
	LEFT JOIN last_year_scd ly ON ly.actorid = ty.actorid
	WHERE (
		ty.quality_class <> ly.quality_class
		OR ty.is_active <> ly.is_active
	)
),
unnested_changed_records as (
	SELECT
		actorname,
		(records::scd_type).quality_class,
		(records::scd_type).is_active,
		(records::scd_type).start_date,
		(records::scd_type).end_date
	FROM changed_records
),
new_records AS (
	select
		ty.actorname,
		ty.quality_class,
		ty.is_active,
		ty.current_year as start_date,
		ty.current_year as end_date
	FROM
		this_year_data ty
	LEFT JOIN last_year_scd ly ON ty.actorid = ly.actorid
	WHERE ly.actorid IS NULL
)
SELECT 
	*, 2022 AS current_year
FROM (
	SELECT * FROM historical_scd
	UNION ALL
	SELECT * FROM unchanged_records
	UNION ALL
	SELECT * FROM unnested_changed_records
	UNION ALL
	SELECT * FROM new_records
) AS final_scd;