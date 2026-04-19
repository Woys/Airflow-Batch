# Airflow-Batch

**Tools & Tech Stack:** Python, Apache Airflow, AWS S3, Pandas, Docker

**TL;DR:** Reusable batch orchestration project built with Apache Airflow. The current implementation includes a Spotify ingestion example and can be adapted for other batch data sources.

**Daily Updated Kaggle Dataset:** [https://www.kaggle.com](https://www.kaggle.com/datasets/daniilmiheev/top-spotify-podcasts-daily-updated)

## Sources

- **Top Podcast Charts:** [https://podcastcharts.byspotify.com/](https://podcastcharts.byspotify.com/)
- **Spotify API Endpoint Used:** [Get an Episode](https://developer.spotify.com/documentation/web-api/reference/get-an-episode)

## Dataset Details

- **Regions Covered:** Argentina (`ar`), Australia (`au`), Austria (`at`), Brazil (`br`), Canada (`ca`), Chile (`cl`), Colombia (`co`), France (`fr`), Germany (`de`), India (`in`), Indonesia (`id`), Ireland (`ie`), Italy (`it`), Japan (`jp`), Mexico (`mx`), New Zealand (`nz`), Philippines (`ph`), Poland (`pl`), Spain (`es`), Netherlands (`nl`), United Kingdom (`gb`), United States (`us`)
- **Data Formats:** Parquet files and a consolidated CSV file
- **Update Frequency:** Daily
- **Key Fields:**
  - `date`: Date of data collection
  - `region`: Country code
  - `episodeUri`: Unique identifier for the episode on Spotify
  - `id`: Episode ID
  - `episodeName`: Name of the episode
  - `name`: Name of the podcast
  - Additional metadata fields as available from the API

## Monetizable Research Dataset Pipeline

This repository also includes a generic, non-Spotify pipeline for building a daily text dataset with `text-ingest`.

- DAG id: `text_daily_pipeline`
- Schedule: daily (`0 2 * * *`)
- Sources: OpenAlex + Crossref + Hacker News + Federal Register (+ NewsAPI when key is set)
- Output format: Parquet only
- Monetization channel: S3-only

### Output in S3

For each run date (`YYYY-MM-DD`) the DAG publishes:

- `s3://<SP_S3_BUCKET>/datasets/text_daily/dt=<YYYY-MM-DD>/dataset.parquet`
- `s3://<SP_S3_BUCKET>/datasets/text_daily/dt=<YYYY-MM-DD>/manifest.json`
- `s3://<SP_S3_BUCKET>/datasets/text_daily/latest.json`
- `s3://<SP_S3_BUCKET>/datasets/text_daily/merged/dataset.csv` (merged across all daily partitions, human-readable)

Canonical schema fields:
`ingested_at`, `source`, `source_record_id`, `doi`, `title`, `abstract`, `authors`, `publication_date`, `journal`, `topics`, `url`, `language`, `license`, `raw_payload_hash`.

### Required Airflow Variables

- `SP_S3_BUCKET` (required; `DATASET_S3_BUCKET` is also accepted as fallback)
- `TI_SEARCH_TERMS` (JSON array, default: `["data engineering", "machine learning", "airflow"]`)
- `TI_MAX_RECORDS_PER_SOURCE` (default: `200`)
- `TI_REQUEST_DELAY_SECONDS` (default: `1.0`)
- `TI_CROSSREF_EMAIL` (recommended for provider etiquette/policy)
- `TI_OPENALEX_API_KEY` (optional)
- `TI_NEWSAPI_KEY` (optional; enables NewsAPI source when set)
- `TI_NEWSAPI_LANGUAGE` (optional, default: `en`)
- `TI_HN_ITEM_TYPE` (optional, one of: `story`, `comment`, `all`; default: `story`)
- `TI_HN_USE_DATE_SORT` (optional boolean, default: `true`)

S3 uploads use Airflow connection id `aws_conn`.

### Run

Trigger from the UI/API by DAG id `text_daily_pipeline`, or run tests locally:

```bash
make all
```
