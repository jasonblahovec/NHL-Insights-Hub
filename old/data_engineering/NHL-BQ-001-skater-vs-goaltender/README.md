# NHL Data Transformation SQL

## Overview
This SQL script, `NHL-BQ-01: Skater vs. Goaltending by Game`, is designed to transform data from the "ingestion/nhl_api" component of the repository. The script performs several transformations to create a comprehensive dataset that includes skater vs. goaltender statistics at the player-game level. The final output is stored in the `nhl.vw_nhl_skater_vs_goaltender` view.

## Views Created

### 1. `nhl.vw_gameinfo`
This view handles potential duplication in the ingested data, ensuring unique game information.

### 2. `nhl.vw_playerstats`
This view ensures unique player statistics data.

### 3. `nhl.vw_playerinfo`
This view ensures unique player information data.

### 4. `nhl.vw_nhl_skatergamestats`
Intermediate view representing skater-game level data. Use `nhl.vw_nhl_skatergamefinal` downstream.

### 5. `nhl.vw_nhl_skatergamefinal`
Final view for skater-game data with added features such as win/loss indicators, star designations, and game numbers.

### 6. `nhl.vw_nhl_goaliegamestats`
View preparing goaltender-game data, which will be pivoted in the next view.

### 7. `nhl.vw_nhl_gamegoaltending`
Pivoted view where each game/team's goaltending is in a single record, designating the goaltender of record and any other appearances.

### 8. `nhl.vw_nhl_skater_vs_goaltender`
The main output view, combining skater and goaltender data to provide insights into skater vs. goaltender interactions.

### Usage Instructions
1. Create BigQuery tables from the ingested output of the NHL API ingestion provided in the ingestion folder of this repository.
2. Run the included script on the ingested data from "ingestion/nhl_api" in your BigQuery environment.
. Utilize the created views, culminating in `nhl.vw_nhl_skater_vs_goaltender`, for skater vs. goaltender insights at the player-game level.
