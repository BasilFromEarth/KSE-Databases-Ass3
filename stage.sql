USE football_db;

-- Optional: start clean
TRUNCATE goals; TRUNCATE games; TRUNCATE players; TRUNCATE teams; TRUNCATE stadiums;

-- =====================
-- Stadiums (6)
-- =====================
INSERT INTO stadiums (stadium_id, stadium_name, city, capacity) VALUES
  (1, 'NSC Olimpiyskiy', 'Kyiv', 70050),
  (2, 'Arena Lviv', 'Lviv', 34915),
  (3, 'Dnipro Arena', 'Dnipro', 31303),
  (4, 'Slavutych-Arena', 'Zaporizhzhia', 12000),
  (5, 'Metalist Stadium', 'Kharkiv', 40003),
  (6, 'Chornomorets Stadium', 'Odesa', 34000);

-- =====================
-- Teams (6)
-- =====================
INSERT INTO teams (team_id, team_name, founded, stadium_id) VALUES
  (1, 'Dynamo Kyiv',        1927, 1),
  (2, 'Shakhtar Donetsk',   1936, 2),
  (3, 'SC Dnipro-1',        2017, 3),
  (4, 'Zorya Luhansk',      1923, 4),
  (5, 'Metalist Kharkiv',   1925, 5),
  (6, 'Chornomorets Odesa', 1936, 6);

-- =====================
-- Players (20)
-- =====================
INSERT INTO players (player_id, name, surname, preferred_pos, shirt_number, team_id) VALUES
  -- Dynamo Kyiv (team_id = 1)
  (1,  'Andriy',   'Yarmolenko',  'Forward',     7,  1),
  (2,  'Viktor',   'Tsyhankov',   'Midfielder',  15, 1),
  (3,  'Artem',    'Besedin',     'Forward',     41, 1),
  (4,  'Mykola',   'Shaparenko',  'Midfielder',  10, 1),

  -- Shakhtar Donetsk (team_id = 2)
  (5,  'Mykhailo', 'Mudryk',      'Winger',      20, 2),
  (6,  'Taison',   'Barcellos',   'Midfielder',  7,  2),
  (7,  'Lassina',  'Traoré',      'Forward',     9,  2),
  (8,  'Taras',    'Stepanenko',  'Midfielder',  6,  2),

  -- SC Dnipro-1 (team_id = 3)
  (9,  'Artem',    'Dovbyk',      'Forward',     24, 3),
  (10, 'Oleksandr','Pikhalyonok', 'Midfielder',  8,  3),
  (11, 'Valeriy',  'Luchkevych',  'Defender',    19, 3),

  -- Zorya Luhansk (team_id = 4)
  (12, 'Oleksandr','Gladkyy',     'Forward',     11, 4),
  (13, 'Bohdan',   'Lednev',      'Midfielder',  16, 4),
  (14, 'Denys',    'Favorov',     'Defender',    27, 4),

  -- Metalist Kharkiv (team_id = 5)
  (15, 'Oleksiy',  'Shevchenko',  'Goalkeeper',  1,  5),
  (16, 'Serhiy',   'Sydorchuk',   'Midfielder',  5,  5),
  (17, 'Yevhen',   'Seleznyov',   'Forward',     99, 5),

  -- Chornomorets Odesa (team_id = 6)
  (18, 'Oleksiy',  'Antonov',     'Forward',     90, 6),
  (19, 'Vladyslav','Kalitvintsev','Midfielder',  45, 6),
  (20, 'Ivan',     'Bobko',       'Midfielder',  23, 6);

-- =====================
-- Games / Matches (8)
-- =====================
INSERT INTO games (game_date, home_team_id, away_team_id, stadium_id, home_goals, away_goals) VALUES
  ('2025-09-01', 1, 2, 1, 2, 1),  -- Dynamo vs Shakhtar 2-1
  ('2025-09-05', 3, 4, 3, 1, 1),  -- Dnipro-1 vs Zorya 1-1
  ('2025-09-10', 5, 6, 5, 0, 0),  -- Metalist vs Chornomorets 0-0
  ('2025-09-12', 2, 3, 2, 3, 2),  -- Shakhtar vs Dnipro-1 3-2
  ('2025-09-15', 4, 1, 4, 0, 2),  -- Zorya vs Dynamo 0-2
  ('2025-09-20', 6, 2, 6, 1, 1),  -- Chornomorets vs Shakhtar 1-1
  ('2025-09-25', 1, 3, 1, 3, 0),  -- Dynamo vs Dnipro-1 3-0
  ('2025-09-28', 5, 4, 5, 2, 1);  -- Metalist vs Zorya 2-1

-- =====================
-- Goals (per scoring event) (18)
-- scoring_team_id = team that is awarded the goal
-- player_id may be NULL for own goals (none used below)
-- =====================
INSERT INTO goals (goal_id, game_id, scoring_team_id, player_id, minute_scored, is_own_goal, is_penalty) VALUES
  -- Game 1: Dynamo 2-1 Shakhtar
  (1,  1, 1, 1,  12, 0, 0),   -- Yarmolenko
  (2,  1, 2, 7,  54, 0, 0),   -- Traoré
  (3,  1, 1, 3,  77, 0, 1),   -- Besedin

  -- Game 2: Dnipro-1 1-1 Zorya
  (4,  2, 3, 9,  35, 0, 0),   -- Dovbyk
  (5,  2, 4, 12, 80, 0, 0),   -- Gladkyy

  -- Game 3: Metalist 0-0 Chornomorets (no goals)

  -- Game 4: Shakhtar 3-2 Dnipro-1
  (6,  4, 2, 7,   9, 0, 0),   -- Traoré
  (7,  4, 3, 9,  40, 0, 0),   -- Dovbyk
  (8,  4, 2, 5,  56, 0, 1),   -- Mudryk
  (9,  4, 3,10,  70, 0, 0),   -- Pikhalyonok
  (10, 4, 2, 6,  84, 0, 1),   -- Taison

  -- Game 5: Zorya 0-2 Dynamo
  (11, 5, 1, 2,  22, 0, 0),   -- Tsyhankov
  (12, 5, 1, 1,  90, 0, 1),   -- Yarmolenko

  -- Game 6: Chornomorets 1-1 Shakhtar
  (13, 6, 6,18,  31, 0, 0),   -- Antonov
  (14, 6, 2, 8,  65, 0, 0),   -- Stepanenko

  -- Game 7: Dynamo 3-0 Dnipro-1
  (15, 7, 1, 3,  17, 0, 1),   -- Besedin
  (16, 7, 1, 1,  49, 0, 0),   -- Yarmolenko
  (17, 7, 1, 4,  73, 0, 0),   -- Shaparenko

  -- Game 8: Metalist 2-1 Zorya
  (18, 8, 5,17,  28, 0, 1),   -- Seleznyov
  (19, 8, 5,16,  58, 0, 0),   -- Sydorchuk
  (20, 8, 4,12,  88, 0, 0);   -- Gladkyy
