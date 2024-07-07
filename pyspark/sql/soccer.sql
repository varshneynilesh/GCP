CREATE TABLE `<PROJECT_ID>.players.selected_strickers`
(
  player_api_id STRING,
  id STRING,
  player_name STRING,
  player_fifa_api_id STRING,
  birthday STRING,
  height STRING,
  weight STRING,
  weighted_score FLOAT64
);

CREATE TABLE `<PROJECT_ID>.players.players_attributes`
(
  id STRING,
  player_fifa_api_id STRING,
  player_api_id STRING,
  date STRING,
  overall_rating STRING,
  potential STRING,
  preferred_foot STRING,
  attacking_work_rate STRING,
  defensive_work_rate STRING,
  crossing STRING,
  finishing STRING,
  heading_accuracy STRING,
  short_passing STRING,
  volleys STRING,
  dribbling STRING,
  curve STRING,
  free_kick_accuracy STRING,
  long_passing STRING,
  ball_control STRING,
  acceleration STRING,
  sprint_speed STRING,
  agility STRING,
  reactions STRING,
  balance STRING,
  shot_power STRING,
  jumping STRING,
  stamina STRING,
  strength STRING,
  long_shots STRING,
  aggression STRING,
  interceptions STRING,
  positioning STRING,
  vision STRING,
  penalties STRING,
  marking STRING,
  standing_tackle STRING,
  sliding_tackle STRING,
  gk_diving STRING,
  gk_handling STRING,
  gk_kicking STRING,
  gk_positioning STRING,
  gk_reflexes STRING
);

CREATE TABLE `<PROJECT_ID>.players.players`
(
  id STRING,
  player_api_id STRING,
  player_name STRING,
  player_fifa_api_id STRING,
  birthday STRING,
  height STRING,
  weight STRING
);
