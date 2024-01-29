# Ingestion of Play-Level Dataset from NHL API

This repository hosts a comprehensive pipeline for ingesting and processing National Hockey League (NHL) play-by-play data. It is designed to facilitate in-depth analysis of game dynamics, player performances, and team strategies through a structured data processing approach.

## Pipeline Structure

The ingestion pipeline is organized into two primary phases, each contained within its own directory:

1. **01_ingest_from_nhl_api**: This directory contains the script responsible for fetching play-by-play data and other relevant game information from the NHL's official API. It performs the initial steps of data cleansing, transformation, and enrichment to prepare the data for subsequent detailed analysis. The processed data is stored as structured Parquet files, optimized for efficient data processing.

2. **02_format_plays**: The scripts within this directory take the enriched play-by-play data from the first phase and extract detailed information about specific game events such as faceoffs, hits, blocked shots, and goals. It enriches the data further with player-specific details and additional context, producing a dataset that allows for granular analysis.

## Additional Requirements

To ensure the pipeline functions correctly, an `install_requirements.sh` script is provided to set up the necessary environment. This script automates the installation of required packages and dependencies, including:

- Python and Pip: For running the scripts and managing Python packages.
- Requests, Pandas, and PySpark: Essential Python libraries for data fetching, manipulation, and processing.
- Google Cloud SDK: For accessing Google Cloud Storage, Secret Manager, and other GCP services.

## Execution Flow

- **Setup**: Run the `install_requirements.sh` script to install all necessary dependencies and set up the environment.
- **Data Ingestion**: Execute the script in `01_ingest_from_nhl_api` to ingest and initially process the NHL data. This step lays the groundwork for detailed event analysis.
- **Data Processing**: Continue with the scripts in `02_format_plays` to break down the play-by-play events into detailed components for in-depth analysis.

## Purpose and Use Cases

This pipeline serves as a robust platform for comprehensive NHL game data analysis. It enables users to:

- Perform macro-level trend analysis and micro-level event examination.
- Assess player performances across various game situations.
- Investigate team strategies and their effectiveness.
- Conduct statistical analyses with enriched and detailed game event data.

## Getting Started

1. **Environment Preparation**: Ensure a Spark environment is ready and access to Google Cloud Storage is configured.
2. **Dependency Installation**: Run `install_requirements.sh` to set up the necessary libraries and tools.
3. **Data Pipeline Execution**: Follow the pipeline structure, starting with data ingestion in `01_ingest_from_nhl_api` and proceeding to detailed event processing in `02_format_plays`.

## Contributions and Feedback

We welcome contributions to enhance the pipeline's capabilities. Feel free to propose new features, optimizations, or bug fixes through pull requests.
