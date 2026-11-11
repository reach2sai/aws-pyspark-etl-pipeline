# AWS PySpark Shipment ETL Pipeline

A portfolio-ready starter ETL project for shipment analytics. It reads shipment CSV data, validates and deduplicates records, calculates delivery performance, and writes curated Parquet outputs. Runs locally with PySpark; output can later be configured for AWS S3 with appropriate credentials.

## Architecture

CSV shipment events → PySpark validation and deduplication → delivery metrics → Parquet curated dataset → SQL analytics

## Requirements

- Python 3.10+
- Java 17
- PySpark (see `requirements.txt`)

## Quick start

```bash
python -m pip install -r requirements.txt
python src/etl.py --input data/shipments.csv --output output/shipments
```

The pipeline writes Parquet to `output/shipments`. To query the result using Spark SQL, see `sql_queries.sql`.

## Data quality rules

- Drop records missing shipment ID or event timestamp.
- Keep the latest event per shipment ID.
- Reject delivery dates earlier than ship dates.
- Compute transit days and on-time status for delivered shipments.

## AWS extension

For S3 input/output, configure Spark with a compatible Hadoop AWS connector and IAM-based credentials, then pass `s3a://...` paths to the script. S3 access is **not** automatically configured by this starter project.

## Repository layout

- `src/etl.py`: PySpark ETL pipeline
- `data/shipments.csv`: synthetic sample data
- `sql_queries.sql`: sample analytics queries
- `requirements.txt`: dependencies
- `.gitignore`: excludes local artifacts and credentials

All sample records are synthetic. No company or client data is included.
