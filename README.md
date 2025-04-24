# pyspark-data-pipeline

This project demonstrates a PySpark pipeline that:
- Reads JSON customer data
- Performs basic cleaning
- Writes partitioned output to Parquet format
- Can run as a script or interactively via notebook

## Structure
- `data/input/` – raw source files (JSON)
- `scripts/` – batch PySpark job
- `notebooks/` – interactive development and testing
- `data/output/` – partitioned output by country
