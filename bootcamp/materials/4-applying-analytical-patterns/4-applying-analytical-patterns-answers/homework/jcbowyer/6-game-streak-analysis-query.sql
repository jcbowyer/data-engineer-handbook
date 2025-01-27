/*
Game Streak Analysis Query
================================

Homework Question: What is the most games each team has won in a 90 game stretch?

This query analyzes NBA game data to find each team's most successful 90-game stretch.
It uses sliding windows to examine every possible sequence of 90 consecutive games
for each team and identifies the stretch with the most wins.

Key Features:
- Uses sliding windows to calculate wins in every 90-game sequence
- Tracks exact start and end dates of each streak
- Calculates win percentage for the best stretches
- Shows duration of each streak in days
- Orders results by most wins, then alphabetically by team

Implementation Details:
1. raw_games: Gets basic game results with win/loss indicators
2. numbered_games: Numbers games sequentially for each team
3. sliding_windows: Creates 90-game windows and calculates wins
4. team_bests: Identifies maximum wins for each team
5. best_windows: Gets dates for the best performing windows

Output Columns:
- team: Team abbreviation
- wins_in_best_90: Number of wins in best 90-game stretch
- win_percentage: Percentage of games won in the stretch
- stretch_start: Date of first game in the streak
- stretch_end: Date of last game in the streak
- days_in_streak: Duration of the streak in days
*/

WITH raw_games AS (
    -- Get basic game results ordered by date
    SELECT DISTINCT
        g.game_date_est,
        gd.team_id,
        gd.team_abbreviation,
        CASE 
            WHEN (g.home_team_wins = 1 AND gd.team_id = g.home_team_id) OR
                 (g.home_team_wins = 0 AND gd.team_id = g.visitor_team_id) 
            THEN 1 
            ELSE 0 
        END AS is_win
    FROM game_details gd
    JOIN games g ON gd.game_id = g.game_id
    ORDER BY team_id, game_date_est
),
numbered_games AS (
    -- Number games sequentially for each team
    SELECT 
        game_date_est,
        team_id,
        team_abbreviation,
        is_win,
        ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY game_date_est) as game_number
    FROM raw_games
),
sliding_windows AS (
    -- Calculate wins in 90-game sliding windows
    SELECT 
        team_id,
        team_abbreviation,
        game_date_est,
        -- Calculate wins in the 90-game window ending at each game
        SUM(is_win) OVER w as wins_in_90,
        COUNT(*) OVER w as games_in_window,
        FIRST_VALUE(game_date_est) OVER w as window_start_date,
        game_date_est as window_end_date,  -- Current game is the end of the window
        game_number
    FROM numbered_games
    WINDOW w AS (
        PARTITION BY team_id 
        ORDER BY game_number
        ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
    )
),
team_bests AS (
    -- Find the best window for each team
    SELECT 
        team_id,
        team_abbreviation,
        MAX(wins_in_90) as best_wins
    FROM sliding_windows
    WHERE games_in_window = 90  -- Only consider complete 90-game windows
    GROUP BY team_id, team_abbreviation
),
best_windows AS (
    -- Get the dates for the best windows
    SELECT DISTINCT
        sw.team_id,
        sw.team_abbreviation,
        tb.best_wins,
        FIRST_VALUE(sw.window_start_date) OVER (
            PARTITION BY sw.team_id 
            ORDER BY sw.wins_in_90 DESC, sw.game_number
        ) as stretch_start,
        FIRST_VALUE(sw.window_end_date) OVER (
            PARTITION BY sw.team_id 
            ORDER BY sw.wins_in_90 DESC, sw.game_number
        ) as stretch_end
    FROM sliding_windows sw
    JOIN team_bests tb ON 
        sw.team_id = tb.team_id AND 
        sw.wins_in_90 = tb.best_wins
    WHERE sw.games_in_window = 90
)
-- Show each team's best performance with dates
SELECT DISTINCT
    team_abbreviation as team,
    best_wins as wins_in_best_90,
    ROUND(100.0 * best_wins / 90, 1) as win_percentage,
    stretch_start,
    stretch_end,
    stretch_end - stretch_start as days_in_streak
FROM best_windows
ORDER BY 
    wins_in_best_90 DESC,
    team;