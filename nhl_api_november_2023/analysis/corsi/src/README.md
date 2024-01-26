# NHL Corsi Analysis Pipeline

This repository hosts a comprehensive pipeline for conducting Corsi analysis on National Hockey League (NHL) play-by-play data, designed to provide in-depth insights into player and team performance during games.

## Pipeline Overview

The pipeline consists of three main scripts, a setup script, and a helper script, organized to facilitate the end-to-end process of data ingestion, processing, partitioning, and analysis:

1. **01_ingest_from_nhl_api**: Fetches raw NHL play-by-play data, performing initial processing and enrichment to prepare it for detailed analysis.

2. **02_format_plays**: Builds upon the enriched data to extract and format detailed play-by-play events, crucial for calculating Corsi metrics.

3. **Corsi Analysis Script**: The core of the pipeline, encapsulated in the `PlayerGameCorsi` class, computes Corsi metrics, providing insights into shot attempt differentials while players are on the ice.

4. **install_requirements.sh**: Sets up the necessary environment by installing required Python packages and dependencies.

5. **Helper Script for Data Partitioning**: Situated between the output of `02_format_plays` and the Corsi analysis script, this script repartitions the data based on game IDs to optimize for parallel processing and efficient data handling.

## Helper Script Functionality

The helper script plays a pivotal role in optimizing the data structure for the subsequent Corsi analysis by partitioning the datasets based on a hashing mechanism. Key functionalities include:

- **Data Loading**: Reads the enriched play-by-play, forwards, and defense datasets from the specified Google Cloud Storage (GCS) buckets.

- **Partitioning**: Applies a hash function to the `game_id` column and calculates a modulus to determine the partition number, effectively distributing the data across a specified number of partitions.

- **Data Writing**: Saves the partitioned datasets back to GCS, ensuring that the data is structured in a way that enhances the performance of the Corsi analysis.

## Execution Flow

- **Initial Setup**: Run the `install_requirements.sh` script to prepare the environment with all necessary dependencies.

- **Data Ingestion and Formatting**: Execute the scripts within `01_ingest_from_nhl_api` and `02_format_plays` directories to ingest and format the NHL play-by-play data.

- **Data Partitioning**: Use the helper script to repartition the formatted datasets, optimizing them for the analysis phase.

- **Corsi Analysis**: Perform the Corsi metric computation using the `PlayerGameCorsi` class, which leverages the partitioned datasets for efficient processing.

## Purpose and Use Cases

This pipeline is designed to offer a robust framework for Corsi metric analysis, facilitating:

- Detailed performance assessments of players and teams.
- Comparative analysis across games, seasons, and teams.
- Identification of strategic patterns and player impact on game outcomes.

## Getting Started

1. **Environment Preparation**: Ensure a Spark environment is set up and run `install_requirements.sh` for installing dependencies.

2. **Data Processing**: Follow the pipeline, starting with the ingestion script in `01_ingest_from_nhl_api`, then formatting plays in `02_format_plays`, and finally repartitioning data with the helper script.

3. **Corsi Analysis**: With the data optimally partitioned, execute the Corsi analysis script to derive insightful metrics on player and team performance.

## Contributions and Feedback

Contributions to enhance the pipeline's capabilities are welcome, including feature additions, optimizations, or bug fixes. Please submit pull requests or open issues for discussion. For further inquiries, contact the maintainers directly.

