-- --------------------------------------------------------------
-- SQL Query: NHL-BQ-01: Skater vs. Goaltending by Game
-- Description: Creates a set of views from the ingested
--              dataset, culminating in `nhl.vw_nhl_skater_vs_goaltender`,
--              a player_id/game_pk level look at each player with information 
--              on opponents' goaltenders.
-- Author: Jason Blahovec
-- Date: October 21, 2023
-- --------------------------------------------------------------

-- Handle any duplication that may exist in the ingested data
-- by creating these views:
CREATE OR REPLACE VIEW nhl.vw_gameinfo AS
SELECT DISTINCT * FROM nhl.gameinfo;

CREATE OR REPLACE VIEW nhl.vw_playerstats AS
SELECT DISTINCT * FROM nhl.playerstats;

CREATE OR REPLACE VIEW nhl.vw_playerinfo AS
SELECT DISTINCT * FROM nhl.playerinfo;


-- The following two views represent skater-game level data.  nhl_skatergamefinal only should be used downstream
-- as vw_nhl_skatergamestats is an interim output.
CREATE OR REPLACE VIEW `nhl.vw_nhl_skatergamestats` AS
SELECT
  LEFT(ps.game_pk, 4) AS game_season,
  RIGHT(LEFT(ps.game_pk, 6), 2) AS game_type,
  RIGHT(ps.game_pk, 4) AS game_number,
  ps.game_pk,
  ps.player_teamtype,
  ps.player_id,
  ps.player_fullname,
  ps.player_jersey_number,
  ps.player_position,
  SAFE_CAST(CASE WHEN ps.assists = 'None' OR ps.assists IS NULL THEN '0.0' ELSE ps.assists END AS DECIMAL) AS assists,
  SAFE_CAST(CASE WHEN ps.goals = 'None' OR ps.goals IS NULL THEN '0.0' ELSE ps.goals END AS DECIMAL) AS goals,
  SAFE_CAST(CASE WHEN ps.shots = 'None' OR ps.shots IS NULL THEN '0.0' ELSE ps.shots END AS DECIMAL) AS shots,
  SAFE_CAST(CASE WHEN ps.hits = 'None' OR ps.hits IS NULL THEN '0.0' ELSE ps.hits END AS DECIMAL) AS hits,
  SAFE_CAST(CASE WHEN ps.powerPlayGoals = 'None' OR ps.powerPlayGoals IS NULL THEN '0.0' ELSE ps.powerPlayGoals END AS DECIMAL) AS powerPlayGoals,
  SAFE_CAST(CASE WHEN ps.powerPlayAssists = 'None' OR ps.powerPlayAssists IS NULL THEN '0.0' ELSE ps.powerPlayAssists END AS DECIMAL) AS powerPlayAssists,
  SAFE_CAST(CASE WHEN ps.penaltyMinutes = 'None' OR ps.penaltyMinutes IS NULL THEN '0.0' ELSE ps.penaltyMinutes END AS DECIMAL) AS penaltyMinutes,
  SAFE_CAST(CASE WHEN ps.faceoffTaken = 'None' OR ps.faceoffTaken IS NULL THEN '0.0' ELSE ps.faceoffTaken END AS DECIMAL) AS faceoffTaken,
  SAFE_CAST(CASE WHEN ps.faceOffWins = 'None' OR ps.faceOffWins IS NULL THEN '0.0' ELSE ps.faceOffWins END AS DECIMAL) AS faceOffWins,
  SAFE_CAST(CASE WHEN ps.takeaways = 'None' OR ps.takeaways IS NULL THEN '0.0' ELSE ps.takeaways END AS DECIMAL) AS takeaways,
  SAFE_CAST(CASE WHEN ps.giveaways = 'None' OR ps.giveaways IS NULL THEN '0.0' ELSE ps.giveaways END AS DECIMAL) AS giveaways,
  SAFE_CAST(CASE WHEN ps.shortHandedGoals = 'None' OR ps.shortHandedGoals IS NULL THEN '0.0' ELSE ps.shortHandedGoals END AS DECIMAL) AS shortHandedGoals,
  SAFE_CAST(CASE WHEN ps.shortHandedAssists = 'None' OR ps.shortHandedAssists IS NULL THEN '0.0' ELSE ps.shortHandedAssists END AS DECIMAL) AS shortHandedAssists,
  SAFE_CAST(CASE WHEN ps.blocked = 'None' OR ps.blocked IS NULL THEN '0.0' ELSE ps.blocked END AS DECIMAL) AS blocked,
  SAFE_CAST(CASE WHEN ps.plusMinus = 'None' OR ps.plusMinus IS NULL THEN '0.0' ELSE ps.plusMinus END AS DECIMAL) AS plusMinus,
  (CAST(SPLIT(ps.evenTimeOnIce, ':')[0] AS DECIMAL) * 60) + CAST(SPLIT(ps.evenTimeOnIce, ':')[1] AS DECIMAL) AS evenTimeOnIce,
  (CAST(SPLIT(ps.powerPlayTimeOnIce, ':')[0] AS DECIMAL) * 60) + CAST(SPLIT(ps.powerPlayTimeOnIce, ':')[1] AS DECIMAL) AS powerPlayTimeOnIce,
  (CAST(SPLIT(ps.shortHandedTimeOnIce, ':')[0] AS DECIMAL) * 60) + CAST(SPLIT(ps.shortHandedTimeOnIce, ':')[1] AS DECIMAL) AS shortHandedTimeOnIce,
  gi.game_type AS game_type_code,
  gi.season AS season_code,
  gi.game_start,
  gi.team_away_name,
  gi.team_home_name,
  gi.winning_goaltender_id,
  gi.losing_goaltender_id,
  gi.first_star_id,
  gi.second_star_id,
  gi.third_star_id,
  gi.name AS location,
  gi.away_goals,
  gi.home_goals,
  gi.head_coach_away,
  gi.head_coach_home,
  SAFE_CAST(CASE WHEN p1_away_goals = 'None' OR p1_away_goals IS NULL THEN '0.0' ELSE p1_away_goals END AS DECIMAL) AS p1_away_goals,
  SAFE_CAST(CASE WHEN p2_away_goals = 'None' OR p2_away_goals IS NULL THEN '0.0' ELSE p2_away_goals END AS DECIMAL) AS p2_away_goals,
  SAFE_CAST(CASE WHEN p3_away_goals = 'None' OR p3_away_goals IS NULL THEN '0.0' ELSE p3_away_goals END AS DECIMAL) AS p3_away_goals,
  SAFE_CAST(CASE WHEN ot4_away_goals = 'None' OR ot4_away_goals IS NULL THEN '0.0' ELSE ot4_away_goals END AS DECIMAL) AS ot4_away_goals,
  SAFE_CAST(CASE WHEN so_goals_away = 'None' OR so_goals_away IS NULL THEN '0.0' ELSE so_goals_away END AS DECIMAL) AS so_goals_away,
  SAFE_CAST(CASE WHEN p1_home_goals = 'None' OR p1_home_goals IS NULL THEN '0.0' ELSE p1_home_goals END AS DECIMAL) AS p1_home_goals,
  SAFE_CAST(CASE WHEN p2_home_goals = 'None' OR p2_home_goals IS NULL THEN '0.0' ELSE p2_home_goals END AS DECIMAL) AS p2_home_goals,
  SAFE_CAST(CASE WHEN p3_home_goals = 'None' OR p3_home_goals IS NULL THEN '0.0' ELSE p3_home_goals END AS DECIMAL) AS p3_home_goals,
  SAFE_CAST(CASE WHEN ot4_home_goals = 'None' OR ot4_home_goals IS NULL THEN '0.0' ELSE ot4_home_goals END AS DECIMAL) AS ot4_home_goals,
  SAFE_CAST(CASE WHEN so_goals_home = 'None' OR so_goals_home IS NULL THEN '0.0' ELSE so_goals_home END AS DECIMAL) AS so_goals_home
FROM (
  SELECT
    SPLIT(index_val, 'ID')[0] AS game_pk,
    *
  FROM
    nhl.vw_playerstats
  WHERE
    player_rosterStatus in ('Y','I')
    AND evenTimeOnIce <> 'None'
) ps
INNER JOIN nhl.vw_gameinfo gi
ON gi.game_pk = ps.game_pk;



-- Create or replace view for nhl_skatergamefinal
CREATE OR REPLACE VIEW `nhl.vw_nhl_skatergamefinal` AS
SELECT
  *,
  CASE WHEN player_teamtype = regulation_winner THEN 1 ELSE 0 END AS regulation_win,
  CASE WHEN player_teamtype = ot_winner THEN 1 ELSE 0 END AS ot_win,
  CASE WHEN player_teamtype = so_winner THEN 1 ELSE 0 END AS shootout_win,
  CASE WHEN opp_player_type = regulation_winner THEN 1 ELSE 0 END AS regulation_loss,
  CASE WHEN opp_player_type = ot_winner THEN 1 ELSE 0 END AS ot_loss,
  CASE WHEN opp_player_type = so_winner THEN 1 ELSE 0 END AS shootout_loss,
  CASE WHEN player_teamtype IN (regulation_winner, ot_winner, so_winner) THEN 1 ELSE 0 END AS win,
  CASE WHEN opp_player_type IN (ot_winner, so_winner) THEN 1 ELSE 0 END AS overtime_loss,
  CASE WHEN opp_player_type = regulation_winner THEN 1 ELSE 0 END AS loss,
  CASE WHEN SAFE_CAST(player_id AS INT64) = SAFE_CAST(first_star_id AS INT64) THEN 1 ELSE 0 END AS isFirstStar,
  CASE WHEN SAFE_CAST(player_id AS INT64) = SAFE_CAST(second_star_id AS INT64) THEN 1 ELSE 0 END AS isSecondStar,
  CASE WHEN SAFE_CAST(player_id AS INT64) = SAFE_CAST(third_star_id AS INT64) THEN 1 ELSE 0 END AS isThirdStar,
  ROW_NUMBER() OVER (PARTITION BY player_id, game_type ORDER BY game_pk) AS career_game_number,
  ROW_NUMBER() OVER (PARTITION BY player_id, game_type, game_season ORDER BY game_pk) AS season_game_number
FROM (
  SELECT
    *,
    p1_home_goals + p2_home_goals + p3_home_goals AS regulation_home,
    p1_away_goals + p2_away_goals + p3_away_goals AS regulation_away,
    CASE WHEN (p1_home_goals + p2_home_goals + p3_home_goals) > (p1_away_goals + p2_away_goals + p3_away_goals) THEN 'home'
         WHEN (p1_home_goals + p2_home_goals + p3_home_goals) < (p1_away_goals + p2_away_goals + p3_away_goals) THEN 'away'
         ELSE 'ot' END AS regulation_winner,
    CASE WHEN (p1_home_goals + p2_home_goals + p3_home_goals) > (p1_away_goals + p2_away_goals + p3_away_goals)
         OR (p1_home_goals + p2_home_goals + p3_home_goals) < (p1_away_goals + p2_away_goals + p3_away_goals) THEN 'reg'
         ELSE CASE WHEN ot4_home_goals > ot4_away_goals THEN 'home'
                   WHEN ot4_home_goals < ot4_away_goals THEN 'away'
              ELSE 'so' END
         END AS ot_winner,
    CASE WHEN so_goals_home = 0 AND so_goals_away = 0 THEN 'no_so'
         WHEN so_goals_home > so_goals_away THEN 'home'
         WHEN so_goals_home < so_goals_away THEN 'away'
         ELSE 'investigate' END AS so_winner,
    CASE WHEN player_teamtype = 'home' THEN 'away' ELSE 'home' END AS opp_player_type
  FROM nhl.vw_nhl_skatergamestats
);



-- Prepare a view of goaltender-game data.  Because we want to join to 
-- skater level data, these will need to be pivoted into a "team goaltender"
-- record in the next view.
CREATE OR REPLACE VIEW `nhl.vw_nhl_goaliegamestats` AS
SELECT
  LEFT(ps.game_pk, 4) AS game_season,
  RIGHT(LEFT(ps.game_pk, 6), 2) AS game_type,
  RIGHT(ps.game_pk, 4) AS game_number,
  ps.game_pk,
  ps.player_teamtype,
  ps.player_id,
  ps.player_fullname,
  ps.player_rosterStatus,
  ps.player_jersey_number,
  ps.player_position,
  ps.player_handed,
  (CAST(SPLIT(ps.gt_timeOnIce, ':')[0] AS DECIMAL) * 60) + CAST(SPLIT(ps.gt_timeOnIce, ':')[1] AS DECIMAL) AS gt_timeOnIce,
  SAFE_CAST(CASE WHEN ps.gt_assists = 'None' OR ps.assists IS NULL THEN '0.0' ELSE ps.gt_assists END AS DECIMAL) AS gt_assists,
  SAFE_CAST(CASE WHEN ps.gt_goals = 'None' OR ps.gt_goals IS NULL THEN '0.0' ELSE ps.gt_goals END AS DECIMAL) AS gt_goals,
  SAFE_CAST(CASE WHEN ps.gt_pim = 'None' OR ps.gt_pim IS NULL THEN '0.0' ELSE ps.gt_pim END AS DECIMAL) AS gt_pim,
  SAFE_CAST(CASE WHEN ps.gt_shots = 'None' OR ps.gt_shots IS NULL THEN '0.0' ELSE ps.gt_shots END AS DECIMAL) AS gt_shots_faced,
  SAFE_CAST(CASE WHEN ps.gt_saves = 'None' OR ps.gt_saves IS NULL THEN '0.0' ELSE ps.gt_saves END AS DECIMAL) AS gt_saves,
  SAFE_CAST(CASE WHEN ps.gt_powerPlaySaves = 'None' OR ps.gt_powerPlaySaves IS NULL THEN '0.0' ELSE ps.gt_powerPlaySaves END AS DECIMAL) AS gt_powerPlaySaves,
  SAFE_CAST(CASE WHEN ps.gt_shortHandedSaves = 'None' OR ps.gt_shortHandedSaves IS NULL THEN '0.0' ELSE ps.gt_shortHandedSaves END AS DECIMAL) AS gt_shortHandedSaves,
  SAFE_CAST(CASE WHEN ps.gt_evenSaves = 'None' OR ps.gt_evenSaves IS NULL THEN '0.0' ELSE ps.gt_evenSaves END AS DECIMAL) AS gt_evenSaves,
  SAFE_CAST(CASE WHEN ps.gt_shortHandedShotsAgainst = 'None' OR ps.gt_shortHandedShotsAgainst IS NULL THEN '0.0' ELSE ps.gt_shortHandedShotsAgainst END AS DECIMAL) AS gt_shortHandedShotsAgainst,
  SAFE_CAST(CASE WHEN ps.gt_evenShotsAgainst = 'None' OR ps.gt_evenShotsAgainst IS NULL THEN '0.0' ELSE ps.gt_evenShotsAgainst END AS DECIMAL) AS gt_evenShotsAgainst,
  SAFE_CAST(CASE WHEN ps.gt_powerPlayShotsAgainst = 'None' OR ps.gt_powerPlayShotsAgainst IS NULL THEN '0.0' ELSE ps.gt_powerPlayShotsAgainst END AS DECIMAL) AS gt_powerPlayShotsAgainst,
  gt_decision
FROM (
  SELECT
    SPLIT(index_val, 'ID')[0] AS game_pk,
    *
  FROM
    nhl.vw_playerstats
  WHERE
    gt_timeOnIce IS NOT NULL
    AND (player_rosterStatus = 'Y' OR gt_decision IN ('W', 'L'))
    AND player_position = 'Goalie'
) ps;


-- Create or replace view for nhl_gamegoaltending - where each game/team's goaltending is pivoted
-- into a single record, designating the goaltender of record as 'decision_' and any other appearance as
-- 'non_decision_'
CREATE OR REPLACE VIEW `nhl.vw_nhl_gamegoaltending` AS
SELECT
  dec.game_season,
  dec.game_type,
  dec.game_number,
  dec.game_pk,
  dec.player_teamtype,
  dec.player_rosterStatus AS decision_roster_status,
  dec.player_id AS decision_player_id,
  dec.player_fullname AS decision_player_fullname,
  dec.player_jersey_number AS decision_player_jersey_number,
  dec.player_handed AS decision_player_handed,
  dec.gt_timeOnIce AS decision_gt_timeOnIce,
  dec.gt_assists AS decision_gt_assists,
  dec.gt_goals AS decision_gt_goals,
  dec.gt_pim AS decision_gt_pim,
  dec.gt_shots_faced AS decision_gt_shots_faced,
  dec.gt_saves AS decision_gt_saves,
  dec.gt_powerPlaySaves AS decision_gt_powerPlaySaves,
  dec.gt_shortHandedSaves AS decision_gt_shortHandedSaves,
  dec.gt_evenSaves AS decision_gt_evenSaves,
  dec.gt_shortHandedShotsAgainst AS decision_gt_shortHandedShotsAgainst,
  dec.gt_evenShotsAgainst AS decision_gt_evenShotsAgainst,
  dec.gt_powerPlayShotsAgainst AS decision_gt_powerPlayShotsAgainst,
  ndec.player_rosterStatus AS non_decision_roster_status,
  ndec.player_id AS non_decision_player_id,
  ndec.player_fullname AS non_decision_player_fullname,
  ndec.player_jersey_number AS non_decision_player_jersey_number,
  ndec.player_handed AS non_decision_player_handed,
  ndec.gt_timeOnIce AS non_decision_gt_timeOnIce,
  ndec.gt_assists AS non_decision_gt_assists,
  ndec.gt_goals AS non_decision_gt_goals,
  ndec.gt_pim AS non_decision_gt_pim,
  ndec.gt_shots_faced AS non_decision_gt_shots_faced,
  ndec.gt_saves AS non_decision_gt_saves,
  ndec.gt_powerPlaySaves AS non_decision_gt_powerPlaySaves,
  ndec.gt_shortHandedSaves AS non_decision_gt_shortHandedSaves,
  ndec.gt_evenSaves AS non_decision_gt_evenSaves,
  ndec.gt_shortHandedShotsAgainst AS non_decision_gt_shortHandedShotsAgainst,
  ndec.gt_evenShotsAgainst AS non_decision_gt_evenShotsAgainst,
  ndec.gt_powerPlayShotsAgainst AS non_decision_gt_powerPlayShotsAgainst,
  gi.game_type AS game_type_code,
  gi.season AS season_code,
  gi.game_start,
  gi.team_away_name,
  gi.team_home_name,
  gi.winning_goaltender_id,
  gi.losing_goaltender_id,
  gi.first_star_id,
  gi.second_star_id,
  gi.third_star_id,
  gi.name AS location,
  gi.away_goals,
  gi.home_goals,
  gi.head_coach_away,
  gi.head_coach_home,
  SAFE_CAST(CASE WHEN p1_away_goals = 'None' OR p1_away_goals IS NULL THEN '0.0' ELSE p1_away_goals END AS DECIMAL) AS p1_away_goals,
  SAFE_CAST(CASE WHEN p2_away_goals = 'None' OR p2_away_goals IS NULL THEN '0.0' ELSE p2_away_goals END AS DECIMAL) AS p2_away_goals,
  SAFE_CAST(CASE WHEN p3_away_goals = 'None' OR p3_away_goals IS NULL THEN '0.0' ELSE p3_away_goals END AS DECIMAL) AS p3_away_goals,
  SAFE_CAST(CASE WHEN ot4_away_goals = 'None' OR ot4_away_goals IS NULL THEN '0.0' ELSE ot4_away_goals END AS DECIMAL) AS ot4_away_goals,
  SAFE_CAST(CASE WHEN so_goals_away = 'None' OR so_goals_away IS NULL THEN '0.0' ELSE so_goals_away END AS DECIMAL) AS so_goals_away,
  SAFE_CAST(CASE WHEN p1_home_goals = 'None' OR p1_home_goals IS NULL THEN '0.0' ELSE p1_home_goals END AS DECIMAL) AS p1_home_goals,
  SAFE_CAST(CASE WHEN p2_home_goals = 'None' OR p2_home_goals IS NULL THEN '0.0' ELSE p2_home_goals END AS DECIMAL) AS p2_home_goals,
  SAFE_CAST(CASE WHEN p3_home_goals = 'None' OR p3_home_goals IS NULL THEN '0.0' ELSE p3_home_goals END AS DECIMAL) AS p3_home_goals,
  SAFE_CAST(CASE WHEN ot4_home_goals = 'None' OR ot4_home_goals IS NULL THEN '0.0' ELSE ot4_home_goals END AS DECIMAL) AS ot4_home_goals,
  SAFE_CAST(CASE WHEN so_goals_home = 'None' OR so_goals_home IS NULL THEN '0.0' ELSE so_goals_home END AS DECIMAL) AS so_goals_home
FROM
  (
    SELECT * FROM `nhl.vw_nhl_goaliegamestats` WHERE gt_decision IN ('W', 'L')
  ) dec
  LEFT JOIN
  (
    SELECT * FROM `nhl.vw_nhl_goaliegamestats` WHERE gt_decision NOT IN ('W', 'L')
  ) ndec
  ON dec.game_pk = ndec.game_pk
  AND dec.player_teamtype = ndec.player_teamtype
  INNER JOIN nhl.vw_gameinfo gi
  ON gi.game_pk = dec.game_pk;


-- Join the TeamGoaltender-Game information back to the skater level view,
-- giving visibility into skater vs. goaltender insights.
CREATE OR REPLACE VIEW `nhl.vw_nhl_skater_vs_goaltender` AS
  SELECT
  skater_stats.season_code AS game_season,
  skater_stats.game_type_code AS game_type,
  skater_stats.game_number,
  skater_stats.game_pk,
  skater_stats.game_start,
  skater_stats.player_teamtype,
  skater_stats.player_id,
  skater_stats.player_fullname,
  skater_stats.player_jersey_number,
  skater_stats.player_position,
  skater_stats.assists,
  skater_stats.goals,
  skater_stats.shots,
  skater_stats.hits,
  skater_stats.powerPlayGoals,
  skater_stats.powerPlayAssists,
  skater_stats.penaltyMinutes,
  skater_stats.faceoffTaken,
  skater_stats.faceOffWins,
  skater_stats.takeaways,
  skater_stats.giveaways,
  skater_stats.shortHandedGoals,
  skater_stats.shortHandedAssists,
  skater_stats.blocked,
  skater_stats.plusMinus,
  skater_stats.evenTimeOnIce,
  skater_stats.powerPlayTimeOnIce,
  skater_stats.shortHandedTimeOnIce,
  skater_stats.team_away_name,
  skater_stats.team_home_name,
  skater_stats.location,
  skater_stats.away_goals,
  skater_stats.home_goals,
  skater_stats.head_coach_away,
  skater_stats.head_coach_home,
  skater_stats.p1_away_goals,
  skater_stats.p2_away_goals,
  skater_stats.p3_away_goals,
  skater_stats.ot4_away_goals,
  skater_stats.so_goals_away,
  skater_stats.p1_home_goals,
  skater_stats.p2_home_goals,
  skater_stats.p3_home_goals,
  skater_stats.ot4_home_goals,
  skater_stats.so_goals_home,
  skater_stats.regulation_away,
  skater_stats.regulation_home,
  skater_stats.regulation_win,
  skater_stats.ot_win,
  skater_stats.shootout_win,
  skater_stats.regulation_loss,
  skater_stats.ot_loss,
  skater_stats.shootout_loss,
  skater_stats.win,
  skater_stats.overtime_loss,
  skater_stats.loss,
  skater_stats.isFirstStar,
  skater_stats.isSecondStar,
  skater_stats.isThirdStar,
  skater_stats.career_game_number,
  skater_stats.season_game_number,
  gt.decision_roster_status,
  gt.decision_player_id,
  gt.decision_player_fullname,
  gt.decision_player_jersey_number,
  gt.decision_player_handed,
  gt.decision_gt_timeOnIce,
  gt.decision_gt_assists,
  gt.decision_gt_goals,
  gt.decision_gt_pim,
  gt.decision_gt_shots_faced,
  gt.decision_gt_saves,
  gt.decision_gt_powerPlaySaves,
  gt.decision_gt_shortHandedSaves,
  gt.decision_gt_evenSaves,
  gt.decision_gt_powerPlayShotsAgainst,
  gt.decision_gt_shortHandedShotsAgainst,
  gt.decision_gt_evenShotsAgainst,
  gt.non_decision_roster_status,
  gt.non_decision_player_id,
  gt.non_decision_player_fullname,
  gt.non_decision_player_jersey_number,
  gt.non_decision_player_handed,
  gt.non_decision_gt_timeOnIce,
  gt.non_decision_gt_assists,
  gt.non_decision_gt_goals,
  gt.non_decision_gt_pim,
  gt.non_decision_gt_shots_faced,
  gt.non_decision_gt_saves,
  gt.non_decision_gt_powerPlaySaves,
  gt.non_decision_gt_shortHandedSaves,
  gt.non_decision_gt_evenSaves,
  gt.non_decision_gt_powerPlayShotsAgainst,
  gt.non_decision_gt_shortHandedShotsAgainst,
  gt.non_decision_gt_evenShotsAgainst
FROM
  `nhl.vw_nhl_skatergamefinal` skater_stats
LEFT JOIN `nhl.vw_nhl_gamegoaltending` gt
ON skater_stats.opp_player_type = gt.player_teamtype
   AND skater_stats.game_pk = gt.game_pk;

