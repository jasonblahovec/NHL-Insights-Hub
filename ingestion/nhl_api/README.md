# NHL Data Ingestion to GCP

This repository contains a Python script for ingesting NHL (National Hockey League) data from the NHL API and storing it in Google Cloud Storage (GCS). The script retrieves information about NHL games, teams, players, and officials and saves it in Parquet format within a GCS bucket.

## NHL API Ingestion - Usage:

To use the NHL API integration with GCP, follow the steps below.  You will need a Google Cloud Project initialized to execute the code on GCP.  THis integration uses the nhl_api folder in the repo.

1. **Clone the Repository**

   Clone this repository to your local machine:

   ```bash
   git clone https://github.com/jasonblahovec/nhl-data-ingestion.git
   cd nhl-data-ingestion
2. **Make a Google Cloud Storage Bucket**
   <gcp-bucket> is where output parquet files will be dropped.
3. **Upload required files to GCS**
   Both extract_nhl_season.py and install_requirements.sh are to be loaded to the bucket created in step 2 - these paths will be used in steps 4 and 5.
4. **Create a Dataproc Cluster**
   install_requirements.sh from step 3 contains libraries required to run the ingestion script.  Below is a light configuration suitable for handling batches of at most 3 seasons:
   
   ```bash
   gcloud dataproc clusters create my-cluster  \
      --initialization-actions gs://<your-bucket-name>/install_requirements.sh  \ # Uploaded in step 3
      --region= <your-region>  \
      --num-workers 2 \
      --worker-boot-disk-size 30GB \
      --master-boot-disk-size 30GB \
      --properties "spark:spark.driver.memory=2g,spark:spark.executor.memory=2g"
5. **Run the Script**
Run the script by providing the necessary arguments:
   ```bash
   gcloud dataproc jobs submit pyspark \
      --cluster=my-cluster \
      --region= <your-region> \
      -- gs://<your-bucket-name>/<path-to-files>/extract_nhl_season.py  \ # Uploaded in step 3
         -- --yearmin 2020 \
            --yearmax 2021 \
            --output_bucket <your-bucket-name> \
            --output_destination output \
            --write_mode append

<start_year> and <end_year>: The range of NHL seasons you want to retrieve data for (e.g., 2020 and 2021).
<bucket_name>: The name of your Google Cloud Storage bucket where the data will be stored.
<destination_path>: The path within the bucket where the data will be saved.
<write_mode>: Specify either "overwrite" or "append" to control how the data should be written to GCS.

6. **(Optional) Upload Output to BigQuery**
   The output parquet can be individually added to Big Query as shown in this example using the output gameinfo table.
   First create a dataset:
   ```bash
         bq mk --location=<gcp-region> <your-gcp-project>:<your-new-bq-dataset>
   Next, run the following commands to load the output of the NHL API ingestion:
   ```bash
         bq load --location="<gcp-region>" --source_format=PARQUET nhl.gameinfo "gs://<your-gcp-bucket>/output/gameinfo/*.parquet"
         bq load --location="<gcp-region>" --source_format=PARQUET nhl.playerinfo "gs://<your-gcp-bucket>/output/playerinfo/*.parquet"
         bq load --location="<gcp-region>" --source_format=PARQUET nhl.playerstats "gs://<your-gcp-bucket>/output/playerstats/*.parquet"
         bq load --location="<gcp-region>" --source_format=PARQUET nhl.officials "gs://<your-gcp-bucket>/output/officials/*.parquet"
         bq load --location="<gcp-region>" --source_format=PARQUET nhl.teams "gs://<your-gcp-bucket>/output/teams/*.parquet"

## Disclaimer
This script is designed to ingest NHL data from the NHL API, and its functionality may be subject to changes or updates by the NHL or Google Cloud. Make sure to comply with any terms of use and API rate limits imposed by the NHL API when using this script.

Furthermore, the NHL API's information is not complete and missing values have been observed in this dataset. Inconsistencies are expected but will be resolved as they are identified. 

## Future Work
As of this update, multi-overtime playoff games are truncated after the first overtime, to be resolved.
Files found in the folder nhlhutbuilder are Databricks Notebooks compatible with Databricks Community edition.  They scrape nhlhutbuilder.com for yearly EA NHL Player ratings. This output will eventually be appended to the dataset for use as a player level feature.

Feel free to use this script to generate data for your NHL data analysis projects. Enjoy working with NHL data!
