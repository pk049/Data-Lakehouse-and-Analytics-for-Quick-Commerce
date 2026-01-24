def get_weight(hour):
    if 7 <= hour < 10:
        return 1.2
    elif 10 <= hour < 12:
        return 1.0
    elif 12 <= hour < 14:
        return 2.2
    elif 14 <= hour < 16:
        return 0.8
    elif 16 <= hour < 19:
        return 1.3
    elif 19 <= hour < 22:
        return 2.8
    elif 22 <= hour < 24:
        return 0.6
    else:  # 00–07
        return 0.2




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

TOPIC = "order_placed_bronze"
BASE_RATE = 20
SIM_START = datetime(2026, 1, 22, 0, 0)

cities = ["Bangalore", "Mumbai", "Delhi"]
zones = ["HSR", "Andheri", "Dwarka"]
payments = ["UPI", "CARD", "COD"]
platforms = ["Android", "iOS", "Web"]

for minute in range(24 * 60):
    event_time = SIM_START + timedelta(minutes=minute)
    weight = get_weight(event_time.hour)
    orders = int(BASE_RATE * weight)

    for _ in range(orders):
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "order_placed",
            "event_time": event_time.isoformat() + "Z",
            "ingest_time": datetime.utcnow().isoformat() + "Z",

            "order_id": f"ord_{uuid.uuid4().hex[:8]}",
            "user_id": f"usr_{random.randint(1000, 9999)}",

            "city": random.choice(cities),
            "zone": random.choice(zones),

            "order_amount": round(random.uniform(100, 800), 2),
            "item_count": random.randint(1, 10),

            "payment_method": random.choice(payments),
            "platform": random.choice(platforms)
        }

        producer.send(TOPIC, value=event)

    producer.flush()
    time.sleep(0.2)  # fast-forward



# kafka-topics.sh --zookeeper localhost:2181 --create --topic order_placed_bronze --replication-factor 1 --partitions 1

# kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic order_placed_bronze
