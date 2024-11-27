from paho.mqtt import client as mqtt_client
from kafka import KafkaProducer
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# MQTT Parameters
mqtt_broker = 'mqtt.hsl.fi'
mqtt_port = 1883
mqtt_topic = "/hfp/v2/journey/ongoing/vp/#"
mqtt_client_id = "MQTT-Python"

# Kafka Parameters
kafka_broker = 'spark-kafka-kafka-1:9092'  # Kafka container name and port
kafka_topic = 'mqtt-data'

# Initialize Kafka Producer
try:
    producer = KafkaProducer(
        bootstrap_servers=[kafka_broker],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize messages as JSON strings
    )
    logging.info("Connected to Kafka Broker")
except Exception as e:
    logging.error(f"Failed to connect to Kafka Broker: {e}")
    producer = None


def connect_mqtt() -> mqtt_client.Client:
    """Connects to the MQTT broker and returns the client instance."""

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            logging.info("Connected to MQTT Broker successfully")
        else:
            logging.error(f"Failed to connect, return code {rc}")

    client = mqtt_client.Client(mqtt_client_id)
    client.on_connect = on_connect
    try:
        client.connect(mqtt_broker, mqtt_port)
    except Exception as e:
        logging.error(f"Failed to connect to MQTT broker: {e}")
    return client


def subscribe(client: mqtt_client.Client):
    """Subscribes to the MQTT topic and publishes messages to Kafka."""

    def on_message(client, userdata, msg):
        try:
            # Decode the MQTT message
            message = msg.payload.decode()
            topic = msg.topic
            logging.info(f"Received `{message}` from topic `{topic}`")

            # Publish the message to Kafka
            if producer is not None:
                producer.send(kafka_topic, value={"topic": topic, "message": message})
                logging.info(f"Published to Kafka topic `{kafka_topic}`")
            else:
                logging.error("Kafka producer is not initialized, unable to send message.")
        except Exception as e:
            logging.error(f"Error while processing message: {e}")

    client.subscribe(mqtt_topic)
    client.on_message = on_message


def run():
    """Runs the MQTT client loop to receive and publish messages."""
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()


if __name__ == "__main__":
    run()
