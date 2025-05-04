import json
import time
import random
from kafka import KafkaProducer
import logging
import sys
from prometheus_client import Counter, Gauge, start_http_server

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global variables (replacing class attributes)
sensors = ["S123", "S456", "S789", "S101", "S112"]
congestion_levels = ["LOW", "MEDIUM", "HIGH"]

# Define Prometheus metrics
vehicle_count = Counter(
    "traffic_vehicle_count_total",
    "Total vehicle count per sensor",
    ["sensor_id"],
)
speed_gauge = Gauge(
    "traffic_speed",
    "Average speed per sensor",
    ["sensor_id"]
)
congestion_counter = Counter(
    "traffic_congestion_events_total",
    "Total congestion events per level and sensor",
    ["sensor_id", "congestion_level"],
)

# Initialize Kafka Producer
try:
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks=1,
    )
except Exception as e:
    logger.error(f"Failed to initialize Kafka Producer: {e}")
    sys.exit(1)

def generate_traffic_event():
    """Generate a random traffic event."""
    return {
        "sensor_id": random.choice(sensors),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "vehicle_count": random.randint(0, 50),
        "speed": round(random.uniform(10, 80), 1),
        "congestion_level": random.choice(congestion_levels),
    }

def run_producer():
    """Run the producer to send traffic data and update metrics."""
    # Start Prometheus metrics server on port 8000
    start_http_server(8000)
    logger.info("Starting Kafka Producer to simulate traffic data...")
    try:
        while True:
            event = generate_traffic_event()
            producer.send("traffic_data", value=event)
            # Update Prometheus metrics
            vehicle_count.labels(sensor_id=event["sensor_id"]).inc(event["vehicle_count"])
            speed_gauge.labels(sensor_id=event["sensor_id"]).set(event["speed"])
            congestion_counter.labels(
                sensor_id=event["sensor_id"],
                congestion_level=event["congestion_level"],
            ).inc()
            logger.info(f"Sent: {event}")
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down producer...")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    run_producer()