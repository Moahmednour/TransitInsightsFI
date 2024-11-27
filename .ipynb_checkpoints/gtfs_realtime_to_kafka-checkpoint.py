import requests
from kafka import KafkaProducer
from google.transit import gtfs_realtime_pb2
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Kafka Configuration
KAFKA_BROKER = "spark-kafka-kafka-1:9092"
TRIP_UPDATES_TOPIC = "trip-updates"
SERVICE_ALERTS_TOPIC = "service-alerts"

# GTFS-RT Endpoints
TRIP_UPDATES_URL = "https://realtime.hsl.fi/realtime/trip-updates/v2/hsl"
SERVICE_ALERTS_URL = "https://realtime.hsl.fi/realtime/service-alerts/v2/hsl"

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_gtfs_rt_feed(url):
    """Fetch GTFS-RT feed from the given URL."""
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        return response.content
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch GTFS-RT feed from {url}: {e}")
        return None

def process_trip_updates(data):
    """Process trip updates feed."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(data)

    for entity in feed.entity:
        if entity.trip_update:
            trip_data = {
                "trip_id": entity.trip_update.trip.trip_id,
                "route_id": entity.trip_update.trip.route_id,
                "start_time": entity.trip_update.trip.start_time,
                "start_date": entity.trip_update.trip.start_date,
                "stop_time_updates": [
                    {
                        "stop_id": update.stop_id,
                        "arrival_time": update.arrival.time,
                        "departure_time": update.departure.time
                    }
                    for update in entity.trip_update.stop_time_update
                ]
            }
            producer.send(TRIP_UPDATES_TOPIC, value=trip_data)
            logging.info(f"Published trip update: {trip_data}")

def process_service_alerts(data):
    """Process service alerts feed."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(data)

    for entity in feed.entity:
        if entity.alert:
            alert_data = {
                "active_period": [
                    {"start": ap.start, "end": ap.end}
                    for ap in entity.alert.active_period
                ],
                "informed_entity": [
                    {"agency_id": ie.agency_id, "route_id": ie.route_id, "stop_id": ie.stop_id}
                    for ie in entity.alert.informed_entity
                ],
                "header_text": entity.alert.header_text.translation[0].text if entity.alert.header_text.translation else "",
                "description_text": entity.alert.description_text.translation[0].text if entity.alert.description_text.translation else ""
            }
            producer.send(SERVICE_ALERTS_TOPIC, value=alert_data)
            logging.info(f"Published service alert: {alert_data}")

def run():
    """Fetch and process Trip Updates and Service Alerts."""
    while True:
        try:
            # Fetch and process Trip Updates
            trip_updates_data = fetch_gtfs_rt_feed(TRIP_UPDATES_URL)
            if trip_updates_data:
                process_trip_updates(trip_updates_data)

            # Fetch and process Service Alerts
            service_alerts_data = fetch_gtfs_rt_feed(SERVICE_ALERTS_URL)
            if service_alerts_data:
                process_service_alerts(service_alerts_data)

        except Exception as e:
            logging.error(f"Error during GTFS-RT processing: {e}")

        finally:
            import time
            time.sleep(15)  # Adjust based on feed update intervals

if __name__ == "__main__":
    run()