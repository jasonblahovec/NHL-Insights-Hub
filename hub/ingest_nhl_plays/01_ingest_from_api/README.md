# NHL Data Ingestion to GCP

This project is designed to ingest NHL game data, including player stats and play-by-play information, into Google Cloud Platform (GCP). It utilizes APIs to fetch data, processes it using PySpark, and stores the result in Google Cloud Storage (GCS) in Parquet format.

## Features

- **Data Extraction**: Extracts detailed player statistics and play-by-play data for NHL games from the official API.
- **Data Processing**: Utilizes PySpark for efficient data transformation and processing, including handling of player on-ice information and game event details.
- **Data Storage**: Stores the processed data in GCS, allowing for scalable and secure data management within the cloud environment.

## Dependencies

- `requests`: For making HTTP requests to the NHL API.
- `pandas`: For data manipulation and analysis.
- `pyspark`: For large-scale data processing.
- `BeautifulSoup`: For HTML parsing to extract text data from web pages.

## Usage

1. **Setup PySpark Environment**: Ensure PySpark is installed and properly configured in your environment.
2. **Configuration**: Use the argument parser to configure the range of NHL game IDs to process, the GCS bucket, and the destination within the bucket for the output data.
3. **Execution**: Run the script to fetch, process, and store NHL game data. The script handles errors gracefully and logs any game IDs that failed to process.

## Data Processing Details

- **Player Statistics**: Fetches and processes player statistics, including goals, assists, points, and more, categorizing them into forwards, defense, and goalies.
- **Play-by-Play Data**: Extracts detailed play-by-play event data from games, including game states, periods, and on-ice player information.
- **Data Enrichment**: Enhances play-by-play data with additional context, such as player positions and on-ice situations, using custom PySpark UDFs (User Defined Functions).
- **Data Storage**: Processes and stores the data in a structured format, ready for analysis and further processing in GCP.

## Output Structure

The processed data is stored in GCS in the following structure:

- `raw_plays`: Contains basic play-by-play event data.
- `raw_forwards`: Contains player statistics for forwards.
- `raw_defense`: Contains player statistics for defensemen.
- `raw_goalies`: Contains player statistics for goalies.
- `plays_with_onice`: Contains enriched play-by-play data with detailed on-ice player information.

## Error Handling

The script logs any game IDs that fail to process, allowing for easy identification and troubleshooting of problematic data.

