DROP DATABASE IF EXISTS football_db;
CREATE DATABASE football_db;

USE football_db;


-- Stadiums
CREATE TABLE stadiums (
  stadium_id   INT PRIMARY KEY,
  stadium_name VARCHAR(80) NOT NULL UNIQUE,
  city         VARCHAR(50),
  capacity     INT UNSIGNED
);

-- Teams
CREATE TABLE teams (
  team_id    INT PRIMARY KEY,
  team_name  VARCHAR(50) NOT NULL UNIQUE,
  founded    SMALLINT UNSIGNED,
  stadium_id INT,
  FOREIGN KEY (stadium_id) REFERENCES stadiums(stadium_id)
);


-- Players
CREATE TABLE players (
  player_id     INT PRIMARY KEY,
  name          VARCHAR(50) NOT NULL,
  surname       VARCHAR(50) NOT NULL,
  preferred_pos VARCHAR(30) NULL,
  shirt_number  SMALLINT UNSIGNED,
  team_id       INT,
  FOREIGN KEY (team_id) REFERENCES teams(team_id) 
);


-- Games/Matches
CREATE TABLE games (
  game_id      INT PRIMARY KEY AUTO_INCREMENT,
  game_date    DATE NOT NULL,
  home_team_id INT NOT NULL,
  away_team_id INT NOT NULL,
  stadium_id   INT,
  home_goals   INT UNSIGNED DEFAULT 0,
  away_goals   INT UNSIGNED DEFAULT 0,
  -- FKs
  FOREIGN KEY (home_team_id) REFERENCES teams(team_id),
  FOREIGN KEY (away_team_id) REFERENCES teams(team_id),
  FOREIGN KEY (stadium_id)   REFERENCES stadiums(stadium_id),
  -- sanity checks
  CHECK (home_team_id <> away_team_id),
  CHECK (home_goals >= 0 AND away_goals >= 0)
);

-- Each scoring event (who scored, when, for which team)
CREATE TABLE goals (
  goal_id       INT PRIMARY KEY,
  game_id       INT NOT NULL,
  scoring_team_id INT NOT NULL,
  player_id     INT,          -- allow NULL for own goals without credited player
  minute_scored SMALLINT,     -- 0–120
  is_own_goal   BOOLEAN DEFAULT FALSE,
  is_penalty    BOOLEAN DEFAULT FALSE,
  FOREIGN KEY (game_id)        REFERENCES games(game_id),
  FOREIGN KEY (scoring_team_id) REFERENCES teams(team_id),
  FOREIGN KEY (player_id)      REFERENCES players(player_id)
);

-- Helpful indexes
CREATE INDEX idx_goals_game ON goals(game_id);
CREATE INDEX idx_goals_player ON goals(player_id);
CREATE INDEX idx_games_date ON games(game_date);