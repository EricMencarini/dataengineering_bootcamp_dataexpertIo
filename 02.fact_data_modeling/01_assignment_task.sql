/*
1.A query to deduplicate `game_details` from Day 1 so there's no duplicates
*/

--Looking at every duplication in the table and filtering to remove the duplicates
WITH deduped AS 
( 
  SELECT 
	*,
	ROW_NUMBER() 
        OVER(PARTITION BY game_id, team_id,player_id) AS rownum
  FROM game_details
)

SELECT 
    *
FROM 
    deduped
WHERE 
    rownum = 1