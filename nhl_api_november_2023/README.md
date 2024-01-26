# NHL Data Analysis Pipeline

This repository hosts a series of data processing and analysis pipelines focusing on NHL game data. The project is structured to facilitate data ingestion from the NHL API, pre-processing of ingested data, and subsequent analytical explorations like the Corsi analysis among other potential analyses.

## Repository Structure

The repository is organized into two main directories:

### 1. `ingest_nhl_plays`

This directory contains scripts for ingesting raw play-by-play data directly from the NHL API and formatting this data into a more analysis-friendly format. It includes:

- `01_ingest_from_nhl_api`: Scripts to retrieve play-by-play data for specified game ID ranges and store the raw data in a cloud storage bucket.
- `02_format_plays`: Scripts to process the raw play-by-play data into a structured format, optimizing it for analytical purposes.

### 2. `analysis`

The `analysis` directory is designed to house various analytical projects that utilize the processed NHL data. Currently, it contains:

- `corsi`: Contains scripts for performing Corsi analysis on the formatted NHL play data. The Corsi analysis is a widely recognized metric in hockey analytics, used to estimate player and team performance.

## Getting Started

To use this pipeline, clone the repository and navigate to the desired analysis or data ingestion script. Each script contains detailed instructions on prerequisites, expected inputs, and execution commands.

## Dependencies

- Python 3.x
- Apache Spark (via Google Cloud Dataproc)
- Required Python libraries: `pyspark`, `pandas`, `requests`, among others listed in the `install_requirements.sh` script.

## Installation

Ensure you have Python installed and set up Google Cloud SDK on your machine. For cluster-based execution, use Google Cloud Dataproc with the provided initialization script (`install_requirements.sh`) to install necessary dependencies on the cluster.

## Usage

1. Start by executing data ingestion scripts within `ingest_nhl_plays` to collect and format the NHL play-by-play data.
2. Move to the `analysis` directory and choose an analysis project. For example, run the Corsi analysis by navigating to the `corsi` folder and following the execution instructions.

## Future Analysis

The `analysis` directory is intended to be expanded with additional analytical projects. Future analyses can be added as separate directories within `analysis`, following a similar structure to the `corsi` project.

## Contributing

Contributions to expand the analytical capabilities of this project are welcome. Please follow standard Git practices for contributing to this repository.
