import json
import time
import uuid
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ===================================================================
def get_weight(hour):
    if 7 <= hour < 10:
        return 12
    elif 10 <= hour < 12:
        return 10
    elif 12 <= hour < 14:
        return 22
    elif 14 <= hour < 16:
        return 8
    elif 16 <= hour < 19:
        return 13
    elif 19 <= hour < 22:
        return 28
    elif 22 <= hour < 24:
        return 6
    else:  # 00–07
        return 2


def inject_noise(event):
    noise_type = random.choice([
        "wrong_amount",
        "negative_qty",
        "missing_field",
    ])

    if noise_type == "wrong_amount":
        event["order_amount"] += random.randint(50, 200)

    elif noise_type == "negative_qty":
        if event["items"]:
            event["items"][0]["quantity"] = -1

    elif noise_type == "missing_field":
        event.pop("payment_method", None)

    return event



# =========================================================================
with open("item_catalog.json") as f:
    ITEM_CATALOG = json.load(f)

for item in ITEM_CATALOG:
    item["weight"] = 5 if int(item["item_id"].split("_")[1]) <= 20 else 1

def pick_items(item_count):
    items = random.choices(
        ITEM_CATALOG,
        weights=[i["weight"] for i in ITEM_CATALOG],
        k=item_count
    )

    order_items = []
    total_amount = 0

    for item in items:
        qty = random.randint(1, 3)
        line_total = item["price"] * qty
        total_amount += line_total

        order_items.append({
            "item_id": item["item_id"],
            "quantity": qty,
            "unit_price": item["price"],
            "line_total": line_total
        })

    return order_items, round(total_amount, 2)




# =========================================================================================
# from pyspark.sql import SparkSession

# spark = (
#     SparkSession.builder
#     .appName("Create-Dim-Item")
#     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
#     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
#     .getOrCreate()
# )

# item_df = spark.createDataFrame(ITEM_CATALOG)

# item_df.write \
#     .format("delta") \
#     .mode("overwrite") \
#     .save("hdfs://localhost:9000/project/dimensions/item_schema/")

# item_df.show(5)
# =====================================================================================================





def pick_items(item_count):
    items = random.choices(
        ITEM_CATALOG,
        weights=[i["weight"] for i in ITEM_CATALOG],
        k=item_count
    )

    order_items = []
    total_amount = 0

    for item in items:
        qty = random.randint(1, 3)
        line_total = item["price"] * qty
        total_amount += line_total

        order_items.append({
            "item_id": item["item_id"],
            "quantity": qty,
            "unit_price": item["price"],
            "line_total": line_total
        })

    return order_items, round(total_amount, 2)

# =========================================================================
TOPIC = "order_placed_bronze"
BASE_RATE = 1
# SIM_START = datetime(2026, 1, 25, 0, 0)  #+timedelta(hours=2)
SIM_START=datetime.now()

cities = ["Bangalore", "Mumbai","Pune"]
zones = {
    "Bangalore":["Cubbon Park","Infantry Road","Whitefield","Majestic","Kanakapura"],
    "Pune":["Hinjewadi","Shivajinagar","Baner","Kothrud","Kharadi"],
    "Mumbai":["Bandra","Powai","Andheri","Ghatkopar","Mulund"]
}
payments = ["UPI", "CARD", "COD"]
platforms = ["Android", "iOS", "Web"]


for minute in range(24 * 60):
    event_time = SIM_START + timedelta(minutes=minute,seconds=random.randint(0, 59))
    weight = get_weight(event_time.hour)
    orders = int(BASE_RATE * weight)

    for _ in range(orders):
        item_count = random.randint(1, 5)
        items, total_amount = pick_items(item_count)

        city=random.choice(cities)
        zone=random.choice(zones[city])
         
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "order_placed",
            "event_time": event_time.isoformat() + "Z",
           

            "order_id": f"ord_{uuid.uuid4().hex[:8]}",
            "user_id": f"usr_{random.randint(1000, 9999)}",

            "city":city,
            "zone":zone,

            "items": items,
            "item_count": len(items),
            "order_amount": total_amount,

            "payment_method": random.choice(payments),
            "platform": random.choice(platforms),

        }

        #  Inject noise only for ~0.5% events
        if random.random() < 0.05:
            event = inject_noise(event)

        producer.send(TOPIC, value=event)
        print(f"event sent are  \n {event}")

    producer.flush()
    time.sleep(1) # (1 Day ----> { 0.5 }*1440=720 seconds=12 minutes)


# kafka-topics.sh --zookeeper localhost:2181 --create --topic order_placed_bronze --replication-factor 1 --partitions 1

# kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic order_placed_bronze

#  "ingest_time": datetime.utcnow().isoformat() + "Z",



# 1sec 1min
# 60sec 60min
# 1min  1hr
# 24min      24hr
