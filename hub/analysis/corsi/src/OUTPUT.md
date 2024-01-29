# Corsi Metrics Output Tables

The Corsi analysis pipeline generates two primary output tables: a summary table for team and player performance over the season and a detailed game-by-game breakdown for each player. These tables provide comprehensive insights into shot attempt differentials, offering a deeper understanding of puck possession and overall team and player performance.

## Team and Player Summary Table

This table aggregates Corsi metrics for each player over the season, offering a high-level overview of performance.

### Schema

- `playerid`: Unique identifier for the player.
- `name`: Name of the player.
- `position`: Playing position of the player (e.g., Forward, Defense).
- `games_played`: Total number of games played by the player.
- `toipg`: Average time on ice per game.
- `corsi_for`: Total shot attempts (goals, shots, missed shots, and blocked shots) by the team while the player was on the ice.
- `corsi_against`: Total shot attempts against the team while the player was on the ice.
- `corsi`: Net shot attempts (Corsi For - Corsi Against).
- `corsi_for_pct`: Percentage of team shot attempts while the player was on the ice (Corsi For / (Corsi For + Corsi Against)).
- `goals_for`: Total goals scored by the team while the player was on the ice.
- `goals_against`: Total goals scored against the team while the player was on the ice.
- `rel_corsi_for`: Corsi For relative to the team's performance when the player is not on the ice.
- `rel_corsi_against`: Corsi Against relative to the team's performance when the player is not on the ice.
- `rel_corsi`: Net relative Corsi (Rel Corsi For - Rel Corsi Against).
- `rel_corsi_for_pct`: Relative Corsi For percentage.
- `corsi_for_relative_pct`: Difference between the player's Corsi For percentage and the team's Corsi For percentage when the player is not on the ice.
- `plusminus`: Plus-minus rating for the player.
- `toi_s`: Total time on ice in seconds.
- `evenstrengthtoi_s`: Total even-strength time on ice in seconds.
- `corsi_for_per_es60`: Corsi For per 60 minutes of even-strength play.
- `corsi_against_per_es60`: Corsi Against per 60 minutes of even-strength play.
- `average_corsi_per_60`: Net Corsi per 60 minutes of even-strength play.

## Player Game-by-Game Detail Table

This table provides a detailed breakdown of Corsi metrics for each player in each game, enabling game-specific performance analysis.

### Schema

The schema includes all fields from the summary table with the addition of:

- `game_id`: Unique identifier for the game.
- `hashmod`: Partition identifier based on a hash of the game_id, used for optimizing data storage and access.

## Usage

These tables are designed for analysts, coaches, and enthusiasts to:

- Evaluate player impact on game outcomes through puck possession metrics.
- Compare player performances across different games or seasons.
- Analyze team strategies and their effectiveness in maintaining puck control.
- Conduct in-depth statistical analyses to inform coaching decisions and player development.

## Accessing the Data

The output tables are stored in Google Cloud Storage in Parquet format and can be accessed using data processing tools that support Parquet, such as Apache Spark, Pandas, or Google BigQuery.

