WITH last_active_year AS (
    SELECT 
        player_name,
        MAX(CASE WHEN is_active THEN current_season ELSE NULL END) as last_active_season
    FROM players
    GROUP BY player_name
),
streak_started AS (
    SELECT 
        p.player_name,
        p.current_season,
        p.scoring_class,
        p.is_active,
        LAG(p.scoring_class, 1) OVER (
            PARTITION BY p.player_name 
            ORDER BY p.current_season
        ) <> p.scoring_class
        OR LAG(p.scoring_class, 1) OVER (
            PARTITION BY p.player_name 
            ORDER BY p.current_season
        ) IS NULL AS did_change
    FROM players p
),
streak_identified AS (
    SELECT
        s.player_name,
        s.scoring_class,
        s.current_season,
        s.is_active,
        SUM(CASE WHEN s.did_change THEN 1 ELSE 0 END) 
            OVER (PARTITION BY s.player_name ORDER BY s.current_season) AS streak_identifier
    FROM streak_started s
),
aggregated AS (
    SELECT
        si.player_name,
        si.scoring_class,
        si.streak_identifier,
        MIN(si.current_season) AS start_date,
        LEAST(MAX(si.current_season), lay.last_active_season) AS end_date,
        bool_or(si.is_active) AS is_active
    FROM streak_identified si
    JOIN last_active_year lay ON si.player_name = lay.player_name
    GROUP BY 
        si.player_name, 
        si.scoring_class, 
        si.streak_identifier,
        lay.last_active_season
)
SELECT 
    player_name,
    scoring_class,
    start_date,
    end_date,
    is_active
FROM aggregated
ORDER BY player_name, start_date;