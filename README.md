Data-Lakehouse-and-Analytics-for-Quick-Commerce

A complete end-to-end Big Data Lakehouse pipeline for a Quick-Commerce platform.
This project simulates item catalog generation, synthetic order streaming, multi-layer data processing (Bronze → Silver → Gold), Hive analysis, and RNN-based demand forecasting using PyTorch.

📂 Project Structure
BIG_DATA_PROJECT/
│
├── Bash_scripts/
│   ├── reset.bash
│   └── start.bash
│
├── Bronze layer/
│   ├── consumers/
│   │   └── orders_save_consumer.py
│   ├── Extras/
│   └── schemas/
│       └── (Schemas for bronze order ingestion)
│
├── Silver_layer/
│   ├── silver_consumer.py
│   └── silver_analysis.py
│
├── Golden layer/
│   ├── item_categories_per_city.py
│   ├── data_for_rnn.py
│   └── rnn_data_read.py
│
├── Producers/
│   ├── generate_item_catalog.py
│   ├── item_catalog.json
│   └── order_producer.py
│
├── Pytorch/
│   └── rnn.py
│
├── Hive/
│   └── (Hive queries for analytical workloads)
│
├── Jars/
│   └── (Delta Lake & Kafka JARs)
│
├── flow.txt
├── item_catalog.json
└── README.md

📝 Overview

This project builds a full data lakehouse pipeline for a Quick-Commerce business.
It uses:

Kafka (streaming orders)

Spark Structured Streaming (ETL: Bronze → Silver → Gold)

Delta Lake (storage & versioning)

Hive (analytics)

PyTorch RNN (demand forecasting)

🔧 Bash Scripts
1️⃣ reset.bash

Deletes all checkpoints and table data from project storage.

Useful for resetting the entire lakehouse pipeline.

2️⃣ start.bash

Starts all necessary services:

Zookeeper

Kafka Broker

Spark Master

Hive Metastore

Beeline

📤 Producers
generate_item_catalog.py

Generates Item Dimension / Catalog

Saves catalog to:

Project folder

HDFS (for downstream pipelines)

order_producer.py

Produces synthetic orders (time-series based)

Publishes messages to Kafka topic: order_placed_bronze

🥉 Bronze Layer
orders_save_consumer.py

Kafka consumer for raw order events

Stores raw unprocessed data in Bronze Delta tables

This is the first consumer to run

Schemas Folder

Contains schema definitions for bronze order ingestion.

🥈 Silver Layer
silver_consumer.py

Reads data from Bronze tables

Performs:

Cleaning

Transformation

Standardization

Stores processed output back into Silver Delta tables

silver_analysis.py

Performs aggregations on Silver tables

Prepares aggregated datasets for RNN training

🥇 Golden Layer
item_categories_per_city.py

Joins Silver data into Gold-level aggregated categories

Generates metrics: item availability per city

data_for_rnn.py

Creates 5-minute windowed timeframes

Produces training sequences for RNN forecasting

Saves data as Delta/JSON for the ML pipeline

rnn_data_read.py

Reads the prepared Gold dataset

Used before RNN model training

🤖 PyTorch RNN
rnn.py

RNN model for demand forecasting

Uses sliding window data from Gold layer

Predicts future order volume per category/city

🐝 Hive

Contains Hive SQL scripts:

Exploratory analysis

Business intelligence queries

Report generation over Gold/Silver tables

📦 Jars

Includes:

Delta Lake JARs

Kafka JARs

Other dependencies used by Spark jobs

📘 flow.txt

Summary of complete project architecture

Data movement sequence

Component interactions

🗂 item_catalog.json

Static export of the generated item catalog.