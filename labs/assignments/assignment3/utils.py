from prometheus_client import start_http_server, Gauge, Counter
import time
import random
from datetime import datetime, timedelta

# Prometheus metrics
VOLUME_GAUGE = Gauge("traffic_volume_total_vehicles", "Total vehicles in 5-min window", ["sensor_id"])
CONGESTION_COUNTER = Counter("congestion_hotspots_total_high", "Cumulative high congestion events", ["sensor_id"])
SPEED_GAUGE = Gauge("average_speed_avg_speed", "Average speed in 10-min window", ["sensor_id"])
SPEED_DROP_GAUGE = Gauge("speed_drops_avg_speed", "Average speed in 2-min window (drops)", ["sensor_id"])
BUSIEST_GAUGE = Gauge("busiest_sensors_total_vehicles", "Total vehicles in 30-min window", ["sensor_id"])

def simulate_traffic_data():
    sensors = ["S123", "S456", "S789"]
    while True:
        timestamp = datetime.now()
        
        # Simulate traffic volume (5-min window)
        for sensor in sensors:
            total_vehicles = random.randint(10, 50)
            VOLUME_GAUGE.labels(sensor_id=sensor).set(total_vehicles)
        
        # Simulate congestion hotspots (cumulative)
        for sensor in sensors:
            if random.random() > 0.8:  # 20% chance of high congestion
                CONGESTION_COUNTER.labels(sensor_id=sensor).inc(random.randint(1, 5))
        
        # Simulate average speed (10-min window)
        for sensor in sensors:
            avg_speed = random.uniform(10, 60)
            SPEED_GAUGE.labels(sensor_id=sensor).set(avg_speed)
        
        # Simulate speed drops (2-min window)
        for sensor in sensors:
            avg_speed = random.uniform(5, 25) if random.random() > 0.7 else random.uniform(25, 60)
            SPEED_DROP_GAUGE.labels(sensor_id=sensor).set(avg_speed)
        
        # Simulate busiest sensors (30-min window)
        for sensor in sensors:
            total_vehicles = random.randint(50, 200)
            BUSIEST_GAUGE.labels(sensor_id=sensor).set(total_vehicles)
        
        print(f"Generated metrics at {timestamp}")
        time.sleep(10)  # Simulate 10-second intervals

if __name__ == "__main__":
    # Start Prometheus metrics server on port 8000
    start_http_server(8000)
    print("Prometheus metrics server started at http://localhost:8000/metrics")
    simulate_traffic_data()