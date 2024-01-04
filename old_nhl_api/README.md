# NHL-Insights-Hub: Pre-November 2023 NHL API
Welcome to the NHL Insights Hub, your central repository for comprehensive data analytics and insights into the world of NHL players and their performance. This project organizes data sourced from the NHL API, EA's NHL game ratings, and more, utilizing a structured data product pipeline to demonstrate data science and engineering capabilities arcoss a range of data platforms.

## Project Structure

Items in this repository are labeled based on the role they play in the data pipeline:

- **Ingestion:** Maintain and ingest NHL player and game data from the NHL API. Web scraping tools for EA player ratings from NHLHUTBUILDER.com.
- **Data Engineering:** BigQuery views enhancing raw data upstream of data science analyses.
- **Data Science:** Python scripts demonstrating various data science capabilities withing the context of the ingested NHL data.
- **Business Intelligence/Visualization:** Placeholder for upcoming visualizations demonstrations.

## Getting Started

Explore each section's folder to dive into detailed documentation and code. Follow the provided instructions to run each script as needed to support your analysis.

## Dependencies

- Python 3.x
- BigQuery (for Data Engineering)
- Databricks (Community edition is supported)
- Additional dependencies specified in each department's documentation.
- Google Cloud Platform

##NOTE: The API used to generate input data was taken out of service in November 2023.  Please see NHL-Insights-Hub/nhl_api_november_2023 for projects that use the new API!