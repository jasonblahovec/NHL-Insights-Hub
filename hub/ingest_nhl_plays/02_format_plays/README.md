# NHL Play-by-Play Detailed Data Processing

This document outlines the processing of NHL play-by-play data to extract detailed information about specific events within a game, such as faceoffs, hits, blocked shots, missed shots, giveaways, penalties, goals, takeaways, and shots on goal.  It is intended to be used in sequence with the outputs of `01_ingest_nhl_api`.

## Process Overview

The script processes enriched play-by-play data to:

1. **Extract Detailed Event Information**: Breaks down each play-by-play event to capture specific details based on the event type (e.g., faceoff winners, hit details).
2. **Enrich Event Data**: Appends additional information to each event, such as team and player details involved in the event.
3. **Output Structured Data**: Stores the enriched and detailed event data for further analysis.

## Functions and Descriptions

### append_lineup_info
- **Purpose**: Enriches event data with lineup information, identifying players involved and their respective teams.
- **Inputs**:
  - `df_stat`: The DataFrame containing the event data.
  - `skater_action`: The action performed by the skater (e.g., hit, shot).
  - `action_label`: A label identifying the action in the DataFrame.

### get_faceoff_detail
- **Purpose**: Extracts detailed information from faceoff events, including the winning team and the players involved.
- **Input**: `df_batch` - The DataFrame containing the batch of play-by-play data.

### get_hit_detail
- **Purpose**: Extracts detailed information from hit events, including the hitter, the hittee, and the location of the hit.
- **Input**: `df_batch` - The DataFrame containing the batch of play-by-play data.

### get_block_detail
- **Purpose**: Extracts detailed information from blocked shot events, including the shooter, the blocker, and the shot type.
- **Input**: `df_batch` - The DataFrame containing the batch of play-by-play data.

### get_miss_detail
- **Purpose**: Extracts detailed information from missed shot events, including the shooter, shot type, result, and shot distance.
- **Input**: `df_batch` - The DataFrame containing the batch of play-by-play data.

### get_giveaway_detail
- **Purpose**: Extracts detailed information from giveaway events, including the player responsible for the giveaway and the zone where it occurred.
- **Input**: `df_batch` - The DataFrame containing the batch of play-by-play data.

### get_penalty_detail
- **Purpose**: Extracts detailed information from penalty events, including the player committing the penalty, the penalty type, and duration.
- **Input**: `df_batch` - The DataFrame containing the batch of play-by-play data.

### get_goal_detail
- **Purpose**: Extracts detailed information from goal events, including the goal scorer, assist providers, shot type, and distance.
- **Input**: `df_batch` - The DataFrame containing the batch of play-by-play data.

### get_takeaway_detail
- **Purpose**: Extracts detailed information from takeaway events, including the player who made the takeaway and the zone where it occurred.
- **Input**: `df_batch` - The DataFrame containing the batch of play-by-play data.

### get_shot_detail
- **Purpose**: Extracts detailed information from shot events, including the shooter, shot type, and distance.
- **Input**: `df_batch` - The DataFrame containing the batch of play-by-play data.

## Output

The script generates a structured dataset containing detailed play-by-play event information, stored as Parquet files in Google Cloud Storage. Each event type is enriched with specific details relevant to the event, providing a comprehensive view of the game's dynamics for analysis.

### Union By-Statistic Output

The processed data for each event type is unioned into a single DataFrame, ensuring a consistent schema across all event types. This unified dataset includes columns for:

- Event type specifics (e.g., `faceoff_winner`, `hit_location`)
- Player and team details involved in each event
- Additional context such as the event zone, shot type, and result

This detailed play-by-play dataset is pivotal for in-depth analysis of game strategies, player performance, and team dynamics.

## Usage

This script is part of a larger pipeline processing NHL game data. It is intended to be run after initial data ingestion and enrichment steps, further breaking down and categorizing play-by-play events for detailed analysis.

For questions or contributions, please refer to the project's GitHub repository.
