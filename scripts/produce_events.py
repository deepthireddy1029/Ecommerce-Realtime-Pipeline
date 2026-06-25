"""Generate fake e-commerce order events and send them to Redpanda (Kafka API)."""

import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "localhost:19092"
TOPIC = "sales_events"

# A small, FIXED product catalog. Reusing the same products (instead of random
# names every time) is what makes later analytics meaningful -- e.g. "top 5
# products by revenue" only works if the same products keep showing up.
PRODUCTS = [
    {"product_id": "P001", "name": "Wireless Mouse",      "category": "Electronics", "price": 24.99},
    {"product_id": "P002", "name": "Mechanical Keyboard", "category": "Electronics", "price": 89.99},
    {"product_id": "P003", "name": "USB-C Hub",           "category": "Electronics", "price": 39.99},
    {"product_id": "P004", "name": "Yoga Mat",            "category": "Fitness",     "price": 29.99},
    {"product_id": "P005", "name": "Steel Water Bottle",  "category": "Fitness",     "price": 14.99},
    {"product_id": "P006", "name": "Coffee Beans 1kg",    "category": "Grocery",     "price": 19.99},
    {"product_id": "P007", "name": "A5 Notebook",         "category": "Stationery",  "price": 6.99},
    {"product_id": "P008", "name": "LED Desk Lamp",       "category": "Home",        "price": 34.99},
]


def delivery_report(err, msg):
    """Runs once per message to confirm it reached the broker (or failed)."""
    if err is not None:
        print(f"  ! delivery failed: {err}")


def make_event():
    """Build one fake 'order placed' event as a dictionary."""
    product = random.choice(PRODUCTS)
    quantity = random.randint(1, 5)
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "order_placed",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "order_id": str(uuid.uuid4()),
        "customer_id": f"C{random.randint(1000, 9999)}",
        "product_id": product["product_id"],
        "product_name": product["name"],
        "category": product["category"],
        "quantity": quantity,
        "unit_price": product["price"],
        "total_amount": round(product["price"] * quantity, 2),
    }


def main():
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    print(f"Sending events to topic '{TOPIC}'. Press Ctrl+C to stop.")
    try:
        while True:
            event = make_event()
            producer.produce(
                TOPIC,
                key=event["product_id"],   # same product -> same partition
                value=json.dumps(event),
                callback=delivery_report,
            )
            producer.poll(0)               # let delivery callbacks fire
            print(f"  -> {event['product_name']} x{event['quantity']} = ${event['total_amount']}")
            time.sleep(random.uniform(0.5, 2.0))
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        producer.flush()                   # wait for any unsent messages
        print("All messages flushed. Bye.")


if __name__ == "__main__":
    main()