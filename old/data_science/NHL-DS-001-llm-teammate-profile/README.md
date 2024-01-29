# NHL Teammate Impact Analysis

## Overview

This demonstration contains a script that combines ingested and refined NHL-Insights-Hub data, hypothesis testing, and Large Language Models (LLMs) to create teammate profiles that can summarize the dynamic between any two provided NHL teammates. The analysis includes data extraction, hypothesis testing, and a LLM summarization layer.

## Components of the Demo:

### 1. **Data Extraction:**

- The `ExtractTeammateData` class uses the NHL skater-vs-goaltender dataset from the data_engineering/NHL-DS-001 folder and extracts relevant data for specified players.
- It provides an EDA (Exploratory Data Analysis) option to summarize player statistics.

### 2. **Hypothesis Testing:**

- The `TeammateImpact` class performs hypothesis testing on various game statistics for two NHL players, considering games played with and without each other.
- It includes nonparametric Mann-Whitney U tests to identify significant differences in player performance, as the integer data does not fit the assumptions of traditional hypothesis testing.

### 3. **LLM Layer:**

- The `TeammateLLMAnalysis` class uses LLM prompting with Langchain to summarize the results of the hypothesis tests in a style that might be presentable on a sports talk show.


## Usage

- In the main method of the included py file, enter the names of the desired teammates as input_player_a and input_player_b, in lower case letters.  Double check that your spelling otherwise matches the player_fullname in ingested NHL API data.  The notebook can be run once the desired names are input.
- Input skater-vs-goaltender data is currently expected to be in a Databricks FileStore location bq_skater_vs_goaltender_2005_2022, but can be changed.

## Output

- Once the notebook is finished processing, the output LM player profiles can be seen in the variable llm_layer.player_profiles.
-You can also view boxplots and histograms for the hypothesis tests that failed using :
    -analysis.display_image_from_bytes(analysis.figures['shots'][player name]['boxplot'])
    -analysis.display_image_from_bytes(analysis.figures['shots'][player name]['histogram'])


## Dependencies

- The scripts use Apache Spark for data processing and analysis.
- Required Python packages are listed in the environment setup section of the scripts.
- The notebook is intended to be run in a Databricks Community Edition environment.

