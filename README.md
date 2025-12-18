# Movie Analytics Pipeline (GCP)

## What this project does
Builds an end-to-end data pipeline that:
- Pulls movie data from an API
- Stores it in Google Cloud Storage
- Processes it in BigQuery
- Trains a ML model to predict movie success
- Visualizes results in Looker Studio

## Technologies
- Python
- Google Cloud Storage
- BigQuery
- BigQuery ML
- Looker Studio

## Folder Structure
- scripts/ → API ingestion
- sql/ → data transformations
- bqml/ → ML models
- schemas/ → table schemas
