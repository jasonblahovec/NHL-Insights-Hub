# Ingestion Folder README

Welcome to the "Ingestion" folder! This repository contains two subfolders, "nhl_api" and "nhlhutbuilder," each serving a distinct purpose for managing and enhancing NHL data. The "nhl_api" subfolder focuses on maintaining an up-to-date NHL database in Google BigQuery, while "nhlhutbuilder" provides a script to scrape EA NHL video game ratings from NHLHUTBUILDER.com, enriching the BigQuery database with additional features.

## Table of Contents
- [nhl_api](#nhl_api)
- [nhlhutbuilder](#nhlhutbuilder)
- [Getting Started](#getting-started)


## nhl_api

The "nhl_api" subfolder contains the code and scripts necessary for maintaining an up-to-date NHL database in Google BigQuery. It facilitates data ingestion, transformation, and loading (ETL) processes, keeping the NHL data repository in sync with the latest updates. This data is invaluable for various analytics, statistics, and machine learning projects related to NHL data.

## nhlhutbuilder

The "nhlhutbuilder" subfolder provides a script designed to be run in Databricks Community Edition. This script is used to scrape player ratings from EA NHL video games available on NHLHUTBUILDER.com. These player ratings can be considered additional features to the existing NHL player data and are crucial for in-depth player analysis, performance evaluation, and building comprehensive player profiles.

By combining the data extracted from NHLHUTBUILDER.com with the data in the "nhl_api" subfolder, you can create a more comprehensive NHL player-level dataset, enhancing the accuracy and depth of your NHL data analysis.

## Getting Started

To get started with the "Ingestion" folder, follow these steps:

1. Clone this repository to your local machine.
2. Navigate to the subfolder relevant to your needs: "nhl_api" or "nhlhutbuilder."
3. Refer to the specific README within each subfolder for detailed instructions on setting up and running the code.


I hope that this NHL data ingestion proves valuable for your NHL data management and analysis needs! If you have any questions or encounter any issues, feel free to reach out to me.
