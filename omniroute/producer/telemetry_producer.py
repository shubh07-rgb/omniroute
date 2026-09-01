import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

from common.fleet_config import VEHICLES, RESTRICTED_ZONES


# ============================================================
# CONFIG
# ============================================================

import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "vehicle-telemetry")
EVENT_INTERVAL_SECONDS = float(os.getenv("EVENT_INTERVAL_SECONDS", "1"))

RESTRICTED_ZONE = RESTRICTED_ZONES[0]


# ============================================================
# KAFKA PRODUCER
# ============================================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)


# ============================================================
# EVENT GENERATION
# ============================================================

def generate_event(vehicle):
    event_type = random.choices(
        ["normal", "speeding", "restricted_zone"],
        weights=[85, 10, 5],
        k=1,
    )[0]

    if event_type == "speeding":
        speed = random.randint(111, 130)
        lat = random.uniform(28.60, 28.65)
        lon = random.uniform(77.15, 77.25)

    elif event_type == "restricted_zone":
        speed = random.randint(40, 90)
        lat = random.uniform(
            RESTRICTED_ZONE["min_lat"],
            RESTRICTED_ZONE["max_lat"],
        )
        lon = random.uniform(
            RESTRICTED_ZONE["min_long"],
            RESTRICTED_ZONE["max_long"],
        )

    else:
        speed = random.randint(40, 100)
        lat = random.uniform(28.60, 28.65)
        lon = random.uniform(77.15, 77.25)

    return {
        "vin": vehicle["vin"],
        "driver_id": vehicle["driver_id"],
        "speed": speed,
        "lat": round(lat, 6),
        "long": round(lon, 6),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ============================================================
# MAIN LOOP
# ============================================================

def main():
    print("Starting OmniRoute telemetry producer...")
    print(f"Kafka: {KAFKA_BOOTSTRAP}")
    print(f"Topic: {TOPIC}")
    print(f"Fleet size: {len(VEHICLES)} vehicles")

    while True:
        for vehicle in VEHICLES:
            event = generate_event(vehicle)

            producer.send(
                TOPIC,
                key=event["vin"].encode("utf-8"),
                value=event,
            )

            print(json.dumps(event))

        producer.flush()

        time.sleep(EVENT_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
