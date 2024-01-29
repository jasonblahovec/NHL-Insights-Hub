# NHL Corsi Analysis Execution Guide

This guide provides step-by-step instructions for executing the NHL Corsi analysis pipeline on Google Cloud Platform (GCP) using Dataproc, a managed Spark and Hadoop service designed to simplify the processing of large datasets.

## Cluster Setup

First, create a Dataproc cluster with the required configurations and initialization actions to install necessary dependencies:

```bash
gcloud dataproc clusters create <cluster-name> \
    --initialization-actions gs://<bucket-name>/install_requirements.sh \
    --region=<region> \
    --num-workers 2 \
    --worker-boot-disk-size 30GB \
    --master-boot-disk-size 30GB \
    --properties "spark:spark.driver.memory=2g,spark:spark.executor.memory=2g"
```

Replace <cluster-name>, <bucket-name>, and <region> with your specific cluster name, Google Cloud Storage (GCS) bucket where the initialization script is stored, and the desired region for the cluster, respectively.

## Data Ingestion
Submit a series of Dataproc jobs to ingest NHL play-by-play data from the NHL API. Adjust the game ID ranges (--gamemin and --gamemax), output bucket (--output_bucket), and destination (--output_destination) as necessary:

```bash
gcloud dataproc jobs submit pyspark --cluster=<cluster-name> --region=<region> \
    -- gs://<bucket-name>/job/ingest_nhl_play_html.py \
    -- --gamemin <start-game-id> --gamemax <end-game-id> \
    --output_bucket <output-bucket> --output_destination <destination-path> --write_mode <write-mode>
```

## Data Preparation
After ingesting raw data, prepare it for Corsi analysis by converting HTML to sparse Parquet format and partitioning the data for efficient processing:

```bash
gcloud dataproc jobs submit pyspark --cluster=<cluster-name> --region=<region> \
    -- gs://<bucket-name>/job/html_to_sparse_parquet.py \
    -- --output_bucket <output-bucket> --output_destination <destination-path>

gcloud dataproc jobs submit pyspark --cluster=<cluster-name> --region=<region> \
    -- gs://<bucket-name>/job/prepare_corsi_input_data.py \
    -- --input_bucket_name <input-bucket> --input_fs_plays <input-plays-path> \
    --input_fs_forwards <input-forwards-path> --input_fs_defense <input-defense-path> \
    --output_bucket_name <output-bucket> --output_fs_plays <output-plays-path> \
    --output_fs_forwards <output-forwards-path> --output_fs_defense <output-defense-path> \
    --hm_count <hashmod-count>
```

## Corsi Analysis
Perform the Corsi analysis for each NHL team and submit the results to BigQuery for further analysis:


```bash
gcloud dataproc jobs submit pyspark --cluster=<cluster-name> --region=<region> \
    -- gs://<bucket-name>/job/dataproc_corsi_from_nhl_plays.py \
    -- --bucket_name <bucket-name> --fs_plays <plays-path> \
    --fs_forwards <forwards-path> --fs_defense <defense-path> \
    --output_location <corsi-output-path> --single_team <team-code> --write_mode <write-mode>
```

Replace <team-code> with the specific NHL team's code for which you want to perform the analysis. Repeat this step for each team, adjusting the --single_team parameter and --write_mode as necessary.

## Loading Results to BigQuery

Finally, load the Corsi analysis results into BigQuery for querying and visualization:

```bash
bq load --location="<region>" --source_format=PARQUET <dataset>.<table> "gs://<bucket-name>/<corsi-output-path>/*.parquet"
```

## Notes

- Ensure that all GCS paths, bucket names, and parameters are correctly set according to your GCP setup.
- Consider using appropriate write modes (overwrite, append) based on your data management strategy.
- Adjust the number of workers, disk sizes, and memory settings of the Dataproc cluster based on the dataset size and computational requirements.
- The --single_team parameter allows for targeted analysis of specific teams. Remove or adjust this parameter to analyze data across all teams.