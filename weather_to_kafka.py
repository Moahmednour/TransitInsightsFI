import requests
import json
from kafka import KafkaProducer
import time

# Configuration
API_KEY = "49236c4c92084f3561bf827a2296f306"  # Replace with your actual API Key
WEATHER_URL = "http://api.openweathermap.org/data/2.5/weather"
KAFKA_BROKER = "spark-kafka-kafka-1:9092"  # Kafka broker address
TOPIC = "weather-data"

# Initialize Kafka Producer
producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Function to Fetch Weather Data
def fetch_weather(lat, lon):
    params = {
        'lat': lat,
        'lon': lon,
        'appid': API_KEY,
        'units': 'metric'  # Get temperature in Celsius
    }
    try:
        response = requests.get(WEATHER_URL, params=params)
        if response.status_code == 200:
            weather_data = response.json()
            return weather_data
        else:
            print(f"Error fetching weather data: {response.status_code}")
            return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

# Function to Publish Weather Data to Kafka
def publish_weather_data(lat, lon):
    weather_data = fetch_weather(lat, lon)
    if weather_data:
        producer.send(TOPIC, value=weather_data)
        print("Weather data sent to Kafka")
    else:
        print("Failed to fetch or send weather data")

# Example Usage: Collect Weather Data Every 30 Seconds
if __name__ == "__main__":
    example_lat = 60.1524422143808
    example_lon = 24.91934076461503  
    while True:
        publish_weather_data(example_lat, example_lon)
        time.sleep(30)  # Fetch data every 30 seconds
