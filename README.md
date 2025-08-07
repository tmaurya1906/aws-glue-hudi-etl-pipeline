# AWS Glue + Apache Hudi ETL Pipeline

This project implements a serverless data lake ETL pipeline using AWS Glue (PySpark) and Apache Hudi.

## 🧠 Objective

To build a scalable and efficient data pipeline that:
- Reads raw CSV files from Amazon S3 (bronze layer)
- Applies basic data transformations using PySpark
- Writes cleaned data as Hudi tables back to S3 (silver layer)
- Registers tables in AWS Glue Data Catalog for Athena querying

## 🛠️ Technologies Used

- AWS Glue (Spark jobs)
- AWS S3
- Apache Hudi
- AWS Glue Catalog
- PySpark
- Athena (for querying)

## 📂 Input Datasets

- `customers.csv`
- `products.csv`
- `transactions.csv`

## 📁 Project Structure

- `glue_job/`: PySpark Glue script for the ETL logic
- `sample_data/`: Sample raw input data (optional)
- `config/`: Glue job parameters and settings

## ⚙️ Glue Job Parameters

| Parameter         | Description                                 |
|------------------|---------------------------------------------|
| `--bucket_name`   | S3 bucket name                              |
| `--ip_path`       | Path to input data (bronze)                 |
| `--op_path`       | Path to output data (silver)                |
| `--datalake-formats` | hudi                                     |
| `--conf`          | Spark configurations for Hudi compatibility|

## 🖼️ Architecture

![Pipeline Diagram](AWS Glue ETL Pipeline Flowchart.png)


