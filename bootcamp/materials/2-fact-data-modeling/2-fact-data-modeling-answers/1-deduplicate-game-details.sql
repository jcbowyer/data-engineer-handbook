WITH RankedRows AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY game_id, player_id 
           ORDER BY player_id DESC
         ) as rn
  FROM game_details
)
SELECT * 
FROM RankedRows 
WHERE rn = 1;