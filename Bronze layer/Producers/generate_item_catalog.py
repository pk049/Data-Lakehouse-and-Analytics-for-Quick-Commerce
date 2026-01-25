import json
import random

random.seed(42)  # 🔒 THIS IS THE KEY

ITEM_CATALOG = [
    {
        "item_id": f"item_{i:03d}",
        "category": random.choice(["Grocery", "Dairy", "Snacks", "Beverages"]),
        "price": random.randint(15,25) * random.choices([3,5,10], weights=[2,4,3], k=1)[0],
    }
    for i in range(1, 50)
]



with open("item_catalog.json", "w") as f:
    json.dump(ITEM_CATALOG, f, indent=2)
