"""Read the order events back out of Redpanda to confirm they're really there."""

import json

from confluent_kafka import Consumer

BOOTSTRAP_SERVERS = "localhost:19092"
TOPIC = "sales_events"


def main():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "stage1-reader",      # consumer group name
        "auto.offset.reset": "earliest",  # start from the oldest message
    })
    consumer.subscribe([TOPIC])
    print(f"Reading from '{TOPIC}'. Press Ctrl+C to stop.")
    try:
        while True:
            msg = consumer.poll(1.0)      # wait up to 1s for a message
            if msg is None:
                continue
            if msg.error():
                print(f"  ! error: {msg.error()}")
                continue
            event = json.loads(msg.value())
            print(f"  <- {event['product_name']} x{event['quantity']} = ${event['total_amount']}")
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()