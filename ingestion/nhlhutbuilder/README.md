# Ingesting EA NHL Player Ratings with Databricks

## Overview

The script called dbc_ingest_batches.py in this folder is intended for use with Databricks Community Edition. It is designed to help you append a subset of "cards" scraped from a provided range of IDs to a destination table. Due to memory constraints with the Community version, scraping must be done in batches. You can control the batch size and number of batches, allowing you to ingest data from NHLHUTBUILDER in a structured manner (or completely, if you have more scalable resources!).

## Getting Started

Before you run this script, make sure you have the necessary libraries and configurations set up in your Databricks environment.

### Prerequisites

- A Databricks Community Edition environment
- Required Python libraries (imported in the script)
- A destination table in your Databricks workspace where you want to collect the scraped data

## Usage

1. Define your scraping parameters in the notebook. The main method is below.  For each game year in the dictionaries skater_ids and goaltender_ids, the required ranges of populated ids are included.  The user should set values of gameyear, batch_size, and n_batches so that the entire range is ingested over some number of runs.  run_skater and run_goaltender are booleans to populate one position group or the other, as the format of attributes for goaltenders is different than that of skters in the dataset. Output will be available in Databricks FileStore at /FileStore/nhl/ea on completion at the destination tables shown below.

   ```python
   if __name__ == "__main__":
       # Define skater and goaltender IDs for different NHL game years
       skater_ids = {
           'NHL20': range(2045, 8900),
           'NHL21': list(range(88811, 99999)) + list(range(8881, 8895)),
           'NHL22': range(1010, 4655),
           'NHL23': range(1000, 5000)
       }
       
       goaltender_ids = {
           'NHL20': range(1000, 3000),
           'NHL21': range(2000, 4200),
           'NHL22': range(1000, 2000),
           'NHL23': range(1000, 2500)
       }
       
       # Specify the NHL game year you want to scrape
       gameyear = list(skater_ids.keys())[3]
       
       # Set batch size and number of batches
       batch_size = 1000
       n_batches = 4
       
       # Configure whether to run the skater and goaltender scraping
       run_skater = True
       skater_destination_table = '/FileStore/nhl/ea/skaters'
       run_goaltender = False
       goaltender_destination_table = '/FileStore/nhl/ea/goaltenders'

   # ... method continues, end of relevant parameters.
