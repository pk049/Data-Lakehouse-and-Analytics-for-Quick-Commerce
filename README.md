# Data-Lakehouse-and-Analytics-for-Quick-Commerce

This project implements a complete end-to-end **Data Lakehouse pipeline** for a Quick-Commerce business.  
It includes item catalog generation, Kafka-based order streaming, multi-layer data processing (Bronze → Silver → Gold) (Medallion Architecture), Hive analytics, and an RNN model for demand forecasting.

---

## Project Structure

```
BIG_DATA_PROJECT/
├── Bash_scripts/
│   ├── reset.bash
│   └── start.bash
├── Bronze layer/
│   ├── consumers/
│   │   └── orders_save_consumer.py
│   ├── Extras/
│   └── schemas/
├── Silver_layer/
│   └── silver_consumer.py
├── Golden layer/
│   ├── item_categories_per_city.py
│   ├── data_for_rnn.py
│   └── rnn_data_read.py
├── Producers/
│   ├── generate_item_catalog.py
│   ├── item_catalog.json
│   └── order_producer.py
├── Pytorch/
│   └── rnn.py
├── Hive/
├── Jars/
├── flow.txt
├── item_catalog.json
└── README.md
```

---

## HDFS Structure

```
/Project
│
├── orders
│   ├── orders_bronze
│   ├── orders_silver
│   └── orders_gold
│       ├── 5min_window
│       └── item_category_per_city
│
└── checkpoints
    ├── orders_bronze
    ├── orders_silver
    └── orders_gold
```

---

## Overview

This project builds a full Data Lakehouse pipeline for streaming and analytical workloads. It uses:

- Kafka for streaming orders  
- Spark Structured Streaming for ETL  
- Delta Lake for storage & versioning  
- Hive for SQL analytics  
- PyTorch RNN for demand forecasting  

---

## How to Run

### 1. Start All Services  
**Run `start.bash` manually:**
```
cd Bash_scripts
bash start.bash
```

This will start:
- Zookeeper  
- Kafka  
- Spark Master  
- Hive Metastore  
- Beeline  

---

### 2. Generate Item Catalog
```
python Producers/generate_item_catalog.py
```

---

### 3. Start Order Producer
```
python Producers/order_producer.py
```
This sends synthetic orders to Kafka topic: `order_placed_bronze`.

---

### 4. Run Bronze Consumer
```
python "Bronze layer/consumers/orders_save_consumer.py"
```

---

### 5. Run Silver Layer
```
python Silver_layer/silver_consumer.py
python Silver_layer/silver_analysis.py
```

---

### 6. Run Gold Layer
```
python "Golden layer/item_categories_per_city.py"
python "Golden layer/data_for_rnn.py"
```

---

### 7. Train RNN Model
```
python Pytorch/rnn.py
```

---

### 8. Reset the Entire Pipeline
```
bash Bash_scripts/reset.bash
```

---

## Additional Files

- `flow.txt` — describes complete pipeline flow  
- `item_catalog.json` — exported item catalog  
- `Hive/` — Hive SQL queries  
- `Jars/` — Delta Lake and Kafka dependencies  

---