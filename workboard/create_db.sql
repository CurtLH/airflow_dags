-- drop the existing database
DROP DATABASE IF EXISTS workboard;

-- recreate the database
CREATE DATABASE workboard;

-- connect to the database
\c workboard

-- create a table for the raw goals
CREATE TABLE goals_raw (
  datetime_load timestamp default CURRENT_TIMESTAMP,
  goal_id INTEGER,
  goal_data JSONB,
  PRIMARY KEY (goal_id)
);

-- create a table for the raw users
CREATE TABLE users_raw (
  datetime_load timestamp default CURRENT_TIMESTAMP,
  user_id INTEGER,
  user_data JSONB,
  PRIMARY KEY (user_id)
);

-- create a table for the raw teams
CREATE TABLE teams_raw (
  datetime_load timestamp default CURRENT_TIMESTAMP,
  team_id INTEGER,
  team_data JSONB,
  PRIMARY KEY (team_id)
);

-- create a table for the raw team members
CREATE TABLE team_members_raw (
  datetime_load timestamp default CURRENT_TIMESTAMP,
  team_id INTEGER,
  team_member_data JSONB,
  PRIMARY KEY (team_id) 
);
