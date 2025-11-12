USE football_db;

SELECT *
FROM games;

-- Queary that incorporates all: 
-- Most recent match in which team from top 3 scorers played, and at least 1 penalty was scored
WITH top3_team_scorers AS (
  SELECT t.team_name
  FROM teams t
  JOIN goals g ON t.team_id = g.scoring_team_id
  GROUP BY t.team_name
  ORDER BY COUNT(g.goal_id) DESC
  LIMIT 3
)

/* Home side is top-3 */
SELECT 
  g.game_date,
  s.stadium_name,
  t1.team_name AS `Home Team`,
  t2.team_name AS `Away Team`,
  g.home_goals,
  g.away_goals,
  SUM(gl.is_penalty) AS penalties_scored
FROM games g
JOIN teams t1 ON t1.team_id = g.home_team_id
JOIN teams t2 ON t2.team_id = g.away_team_id
JOIN goals gl ON gl.game_id = g.game_id
JOIN players p ON p.player_id = gl.player_id
JOIN stadiums s ON s.stadium_id = g.stadium_id
WHERE t1.team_name IN (SELECT team_name FROM top3_team_scorers)
  AND g.game_date = (
        SELECT MAX(g2.game_date)
        FROM games g2
        WHERE g2.stadium_id = g.stadium_id
      )
  AND g.game_id = (
        SELECT MAX(g3.game_id)
        FROM games g3
        WHERE g3.stadium_id = g.stadium_id
          AND g3.game_date = g.game_date
      )
GROUP BY 
  g.game_id, g.game_date, s.stadium_name,
  t1.team_name, t2.team_name, g.home_goals, g.away_goals
HAVING penalties_scored > 0

UNION ALL

/* Away side is top-3 */
SELECT 
  g.game_date,
  s.stadium_name,
  t1.team_name AS `Home Team`,
  t2.team_name AS `Away Team`,
  g.home_goals,
  g.away_goals,
  SUM(gl.is_penalty) AS penalties_scored
FROM games g
JOIN teams t1 ON t1.team_id = g.home_team_id
JOIN teams t2 ON t2.team_id = g.away_team_id
JOIN goals gl ON gl.game_id = g.game_id
JOIN players p ON p.player_id = gl.player_id
JOIN stadiums s ON s.stadium_id = g.stadium_id
WHERE t2.team_name IN (SELECT team_name FROM top3_team_scorers)
  AND g.game_date = (
        SELECT MAX(g2.game_date)
        FROM games g2
        WHERE g2.stadium_id = g.stadium_id
      )
  AND g.game_id = (
        SELECT MAX(g3.game_id)
        FROM games g3
        WHERE g3.stadium_id = g.stadium_id
          AND g3.game_date = g.game_date
      )
GROUP BY 
  g.game_id, g.game_date, s.stadium_name,
  t1.team_name, t2.team_name, g.home_goals, g.away_goals
HAVING penalties_scored > 0

ORDER BY game_date DESC
LIMIT 1;
