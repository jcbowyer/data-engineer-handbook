/*
Game Details Grouping Set Query
================================

This query provides a comprehensive analysis of NBA game details data using GROUPING SETS
to efficiently compute aggregations across multiple dimensions:
1. Player-Team stats (who scored the most points playing for one team?)
2. Player-Season stats (who had the best individual seasons?)
3. Team-level stats (which teams have been most successful?)

Usage examples:
-- For player-team stats (who scored the most points for one team):
SELECT * FROM [this_query] 
WHERE grouping_type = 'Player-Team Stats' 
ORDER BY total_points DESC 
LIMIT 10;

-- For player-season stats (best scoring seasons):
SELECT * FROM [this_query] 
WHERE grouping_type = 'Player-Season Stats' 
ORDER BY avg_points DESC 
LIMIT 10;

-- For team stats (most successful teams):
SELECT * FROM [this_query] 
WHERE grouping_type = 'Team Stats' 
ORDER BY games_positive_plus_minus DESC 
LIMIT 10;
*/

WITH game_seasons AS (
    -- First, we need to determine the season for each game
    -- NBA seasons span calendar years, typically starting in October
    -- So we adjust the season year based on the month of the game
    SELECT 
        g.*,
        EXTRACT(YEAR FROM CASE 
            WHEN EXTRACT(MONTH FROM games.game_date_est) >= 10 THEN 
                games.game_date_est + INTERVAL '3 months'  -- Games in Oct-Dec count for the next year
            ELSE 
                games.game_date_est - INTERVAL '9 months'  -- Games in Jan-Jun count for the current year
        END) AS season
    FROM game_details g
    JOIN games ON g.game_id = games.game_id
),
aggregated_stats AS (
    -- Use GROUPING SETS to efficiently compute multiple aggregation levels
    -- This is more efficient than using separate queries with UNION ALL
    SELECT
        player_id,
        player_name,
        team_id,
        team_abbreviation,
        season,
        -- Basic counting stats
        COUNT(*) as games_played,
        SUM(pts) as total_points,
        ROUND(CAST(AVG(pts) AS numeric), 2) as avg_points,
        SUM(ast) as total_assists,
        ROUND(CAST(AVG(ast) AS numeric), 2) as avg_assists,
        SUM(reb) as total_rebounds,
        ROUND(CAST(AVG(reb) AS numeric), 2) as avg_rebounds,
        -- Advanced metrics
        SUM(CASE WHEN plus_minus > 0 THEN 1 ELSE 0 END) as games_positive_plus_minus,
        ROUND(CAST(AVG(plus_minus) AS numeric), 2) as avg_plus_minus
    FROM game_seasons
    GROUP BY GROUPING SETS (
        (player_id, player_name, team_id, team_abbreviation), -- Player-Team combination stats
        (player_id, player_name, season),                     -- Player stats by season
        (team_id, team_abbreviation)                          -- Overall team stats
    )
)
SELECT 
    player_id,
    player_name,
    team_id,
    team_abbreviation,
    season,
    games_played,
    total_points,
    avg_points,
    total_assists,
    avg_assists,
    total_rebounds,
    avg_rebounds,
    games_positive_plus_minus,
    avg_plus_minus,
    -- Identify the type of grouping based on which columns are NULL
    CASE
        WHEN player_id IS NOT NULL AND team_id IS NOT NULL AND season IS NULL 
            THEN 'Player-Team Stats'
        WHEN player_id IS NOT NULL AND season IS NOT NULL AND team_id IS NULL 
            THEN 'Player-Season Stats'
        WHEN team_id IS NOT NULL AND player_id IS NULL 
            THEN 'Team Stats'
    END as grouping_type
FROM aggregated_stats
WHERE 
    -- Filter out small sample sizes to ensure meaningful statistics
    games_played >= CASE 
        WHEN player_id IS NOT NULL THEN 20  -- Minimum 20 games for player stats
        WHEN team_id IS NOT NULL THEN 82    -- Minimum 82 games (full season) for team stats
        ELSE 0
    END
ORDER BY 
    -- Order results by grouping type first, then by total points
    CASE 
        WHEN player_id IS NOT NULL AND team_id IS NOT NULL AND season IS NULL 
            THEN 1  -- Player-Team stats first
        WHEN player_id IS NOT NULL AND season IS NOT NULL AND team_id IS NULL 
            THEN 2  -- Player-Season stats second
        ELSE 3     -- Team stats last
    END,
    total_points DESC NULLS LAST;