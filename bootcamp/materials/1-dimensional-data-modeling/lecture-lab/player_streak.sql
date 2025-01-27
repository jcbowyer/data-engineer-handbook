SELECT player_name, scoring_class, COUNT(*)
FROM players
WHERE player_name = 'A.C. Green'
GROUP BY player_name, scoring_class
ORDER BY scoring_class;