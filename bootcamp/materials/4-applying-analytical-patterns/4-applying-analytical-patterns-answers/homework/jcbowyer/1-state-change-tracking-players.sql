/* 
Player State Change Tracking Query
================================

This query tracks the state changes of NBA players throughout their careers by analyzing 
their season-by-season activity. It uses a complex series of CTEs to build up the player's
history and determine their state in each season.

Player States
------------
The query classifies players into five states:
1. New: First season a player appears in the league
2. Continued Playing: Player was active in consecutive seasons
3. Retired: Player was active but then became inactive
4. Returned from Retirement: Player became active after being inactive
5. Stayed Retired: Player remains inactive after retiring

Data Structure
-------------
- Input tables:
  * player_seasons: Raw season statistics for each player
  * season_stats: Custom type containing (season, pts, ast, reb, gp)

Query Components
--------------
1. years CTE:
   - Generates the range of seasons to analyze (1996-2022)
   - Ensures we have a continuous timeline for all players

2. p CTE:
   - Finds the first season for each player
   - Used to establish when players entered the league

3. players_and_seasons CTE:
   - Cross-joins players with all seasons after their first appearance
   - Creates a complete timeline for each player's potential career

4. windowed CTE:
   - Creates an array of season statistics for each player
   - Uses window functions to maintain the historical context
   - Tracks points, assists, rebounds, and games played

5. static CTE:
   - Collects unchanging player information (height, college, etc.)
   - Takes the MAX of each field to handle any data inconsistencies

6. player_activity CTE:
   - Determines active/inactive status for each season
   - Uses LAG to compare with previous season's status
   - Essential for detecting state changes

Main Query Features
-----------------
- Scoring classification based on points per game
- Years since last active calculation
- Player state determination using current and previous season status
- Complete player biographical information

Usage Example
------------
SELECT * FROM [this_query] 
ORDER BY player_name, season;

Sample player states progression:
New -> Continued Playing -> Continued Playing -> Retired -> Stayed Retired

Output Columns
-------------
- player_name: Player's name
- height, college, country: Biographical info
- draft_year, draft_round, draft_number: Draft information
- season_stats: Array of detailed season statistics
- scoring_class: Classification based on scoring average
- years_since_last_active: Years since player's last active season
- season: Current season being analyzed
- is_active: Whether player was active in current season
- player_state: Current state classification
*/

WITH years AS (
    SELECT *
    FROM GENERATE_SERIES(1996, 2022) AS season
),
p AS (
    SELECT 
        player_name,
        MIN(season) AS first_season
    FROM player_seasons
    GROUP BY player_name
),
players_and_seasons AS (
    SELECT *
    FROM p
    JOIN years y
        ON p.first_season <= y.season
),
windowed AS (
    SELECT
        pas.player_name,
        pas.season,
        ARRAY_REMOVE(
            ARRAY_AGG(
                CASE
                    WHEN ps.season IS NOT NULL
                        THEN ROW(
                            ps.season,
                            ps.pts,
                            ps.ast,
                            ps.reb,
                            ps.gp
                        )::season_stats
                END)
            OVER (PARTITION BY pas.player_name ORDER BY COALESCE(pas.season, ps.season)),
            NULL
        ) AS seasons
    FROM players_and_seasons pas
    LEFT JOIN player_seasons ps
        ON pas.player_name = ps.player_name
        AND pas.season = ps.season
    ORDER BY pas.player_name, pas.season
),
static AS (
    SELECT
        player_name,
        MAX(height) AS height,
        MAX(college) AS college,
        MAX(country) AS country,
        MAX(draft_year) AS draft_year,
        MAX(draft_round) AS draft_round,
        MAX(draft_number) AS draft_number
    FROM player_seasons
    GROUP BY player_name
),
player_activity AS (
    SELECT 
        w.player_name,
        w.season,
        (seasons[CARDINALITY(seasons)]::season_stats).season = w.season AS is_active,
        LAG((seasons[CARDINALITY(seasons)]::season_stats).season = w.season) 
            OVER (PARTITION BY w.player_name ORDER BY w.season) AS was_active_last_season
    FROM windowed w
)
SELECT 
    w.player_name,
    s.height,
    s.college,
    s.country,
    s.draft_year,
    s.draft_round,
    s.draft_number,
    w.seasons AS season_stats,
    CASE
        WHEN (w.seasons[CARDINALITY(w.seasons)]::season_stats).pts > 20 THEN 'star'
        WHEN (w.seasons[CARDINALITY(w.seasons)]::season_stats).pts > 15 THEN 'good'
        WHEN (w.seasons[CARDINALITY(w.seasons)]::season_stats).pts > 10 THEN 'average'
        ELSE 'bad'
    END::scoring_class AS scoring_class,
    w.season - (w.seasons[CARDINALITY(w.seasons)]::season_stats).season as years_since_last_active,
    w.season,
    (w.seasons[CARDINALITY(w.seasons)]::season_stats).season = w.season AS is_active,
    CASE
        WHEN pa.is_active AND pa.was_active_last_season IS NULL THEN 'New'
        WHEN pa.is_active AND pa.was_active_last_season THEN 'Continued Playing'
        WHEN pa.is_active AND NOT pa.was_active_last_season THEN 'Returned from Retirement'
        WHEN NOT pa.is_active AND pa.was_active_last_season THEN 'Retired'
        WHEN NOT pa.is_active AND NOT pa.was_active_last_season THEN 'Stayed Retired'
    END AS player_state
FROM windowed w 
JOIN static s ON w.player_name = s.player_name
JOIN player_activity pa ON w.player_name = pa.player_name AND w.season = pa.season
WHERE s.player_name = 'Aaron Brooks'
ORDER BY w.player_name, w.season;