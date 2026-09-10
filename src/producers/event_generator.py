import argparse
import json
import random
import sys
import time
from datetime import datetime, timezone
import uuid

try:
    from kafka import KafkaProducer
except ImportError:
    print("❌ ERROR: The 'kafka-python' package is not installed. Run: pip install kafka-python")
    sys.exit(1)

# Synthetic product catalog categorized by department with fixed baseline pricing
CATALOG = {
    "Electronics": [
        {"item_id": "item_101", "name": "Wireless Headphones", "price": 89.99},
        {"item_id": "item_102", "name": "Mechanical Keyboard", "price": 129.50},
        {"item_id": "item_103", "name": "4K Monitor 27-inch", "price": 349.00},
    ],
    "Fashion": [
        {"item_id": "item_201", "name": "Running Shoes", "price": 75.00},
        {"item_id": "item_202", "name": "Cotton Hoodie", "price": 45.00},
        {"item_id": "item_203", "name": "Leather Jacket", "price": 199.99},
    ],
    "Home & Kitchen": [
        {"item_id": "item_301", "name": "Espresso Coffee Machine", "price": 150.00},
        {"item_id": "item_302", "name": "Air Fryer 5L", "price": 99.00},
    ]
}

# User funnel stage definition with realistic probability distribution
# - view_item: 80% (top-of-funnel navigation)
# - add_to_cart: 15% (mid-funnel purchase intent)
# - purchase: 5% (bottom-of-funnel conversion)
EVENT_TYPES = ["view_item", "add_to_cart", "purchase"]
EVENT_WEIGHTS = [0.80, 0.15, 0.05]


def generate_event() -> dict:
    """
    Generates a single synthetic e-commerce clickstream event.

    Returns:
        dict: A dictionary containing event metadata formatted for JSON serialization.
    """
    # Randomly select product category and item from the catalog
    category = random.choice(list(CATALOG.keys()))
    product = random.choice(CATALOG[category])
    
    # Weighted choice to model realistic user conversion behavior
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)
    
    # Assign monetary amount based on event intent
    amount = product["price"] if event_type in ["purchase", "add_to_cart"] else 0.0

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": f"usr_{random.randint(1000, 1050)}",  # Sample pool of 50 recurring users
        "session_id": str(uuid.uuid4())[:8],
        "event_type": event_type,
        "item_id": product["item_id"],
        "item_name": product["name"],
        "category": category,
        "amount": round(amount, 2),
        "timestamp": datetime.now(timezone.utc).isoformat()  # Standard ISO 8601 UTC timestamp
    }


def main():
    """
    Main entry point for the event generator.
    Parses CLI arguments, initializes the Kafka producer, and starts the event stream loop.
    """
    parser = argparse.ArgumentParser(description="Real-Time E-Commerce Kafka Event Producer")
    parser.add_argument("--bootstrap-server", default="localhost:9092", help="Kafka broker address")
    parser.add_argument("--topic", default="ecommerce-events", help="Destination Kafka topic name")
    parser.add_argument("--events-per-sec", type=float, default=2.0, help="Target emission rate in events per second")
    args = parser.parse_args()

    print(f"🚀 Connecting to Kafka broker at: {args.bootstrap_server}")
    print(f"📦 Target Topic: '{args.topic}'")
    print(f"⚡ Emission Rate: {args.events_per_sec} events/second\n")

    # Initialize KafkaProducer with JSON value serializer and delivery guarantees
    try:
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_server,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",     # Ensure full leader and replica acknowledgment
            retries=3       # Retry transient network failures
        )
    except Exception as e:
        print(f"❌ Failed to connect to Kafka broker: {e}")
        sys.exit(1)
    
    count = 0
    delay = 1.0 / args.events_per_sec  # Delay interval between emitted messages in seconds

    # Continuous streaming execution loop
    try:
        while True:
            event = generate_event()
            producer.send(args.topic, value=event)
            count += 1
            print(f"[{count}] Event Sent -> User: {event['user_id']} | Type: {event['event_type']} | Item: {event['item_name']} (${event['amount']})")
            time.sleep(delay)
    except KeyboardInterrupt:
        print("\n🛑 Event generator stopped by user.")
    finally:
        # Flush pending messages and release producer network socket resources
        producer.flush()
        producer.close()
        print(f"✅ Total events successfully published to Kafka: {count}")


if __name__ == "__main__":
    main()