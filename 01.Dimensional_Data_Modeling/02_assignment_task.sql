/*
2. **Cumulative table generation query:** 
Write a query that populates the `actors` table one year at a time.
*/

--Range of years in actor_films table
SELECT MIN(YEAR), MAX(YEAR) FROM actor_films -- min/max 1970/2021

--Coalesce values that are NOT temporal(not changing)
--Seed querie for cumulation;

INSERT INTO actors

WITH last_year AS (
  SELECT * FROM actors
  WHERE current_year = 1969
), current_year AS (
  SELECT 
      actorid,
      actor,
      year,
      ARRAY_AGG(ROW(film, votes, rating, filmid)::films_arr) AS film,
      AVG(rating) AS avg_rating
  FROM 
    actor_films
  WHERE 
    year = 1970
  GROUP BY 
    actorid, actor, year
)
SELECT 
  COALESCE(cy.actorid, ly.actorid) AS actorid,
  COALESCE(cy.actor, ly.actorname) AS actorname,
  COALESCE(ly.films, ARRAY[]::films_arr[] || COALESCE(cy.film, ARRAY[]::films_arr[])) AS films,
  CASE
    WHEN cy.year IS NOT NULL THEN 
      CASE 
        WHEN cy.avg_rating > 8.0 THEN 'star'
        WHEN cy.avg_rating > 7.0 THEN 'good'
        WHEN cy.avg_rating > 6.0 THEN 'average'
        ELSE 'bad'
      END::quality_class
    ELSE ly.quality_class
  END AS quality_class,
  CASE 
    WHEN cy.film IS NOT NULL THEN TRUE 
    ELSE FALSE 
  END AS is_active,
  COALESCE(cy.year, ly.current_year + 1) AS current_year
FROM current_year cy
    FULL OUTER JOIN last_year ly ON ly.actorid = cy.actorid