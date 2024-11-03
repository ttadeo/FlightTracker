import json
import time
import requests
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

avro_schema = {
    "type": "record",
    "name": "FlatFlightDetails",
    "fields": [
        {"name": "geography_latitude", "type": "double"},
        {"name": "geography_longitude", "type": "double"},
        {"name": "geography_altitude", "type": "double"},
        {"name": "geography_direction", "type": "string"},
        {"name": "speed_horizontal", "type": "double"},
        {"name": "speed_vertical", "type": "double"},
        {"name": "departure_iataCode", "type": "string"},
        {"name": "departure_icaoCode", "type": "string"},
        {"name": "arrival_iataCode", "type": "string"},
        {"name": "arrival_icaoCode", "type": "string"},
        {"name": "aircraft_regNumber", "type": "string"},
        {"name": "aircraft_icaoCode", "type": "string"},
        {"name": "aircraft_icao24", "type": "string"},
        {"name": "airline_iataCode", "type": "string"},
        {"name": "airline_icaoCode", "type": "string"},
        {"name": "flight_iataNumber", "type": "string"},
        {"name": "flight_icaoNumber", "type": "string"},
        {"name": "flight_number", "type": "string"},
        {"name": "system_updated", "type": "long"},
        {"name": "system_squawk", "type": "string"},
        {"name": "system_status", "type": "string"}
    ]
}

avro_producer = AvroProducer({
    'bootstrap.servers': '52.203.71.36:9092',
    'schema.registry.url': 'http://52.203.71.36:8081'
    }, default_value_schema=avro.loads(json.dumps(avro_schema)))

def send_to_kafka(topic, data):
    message = {
        "geography_latitude": data['geography'].get('latitude', 0.0),
        "geography_longitude": data['geography'].get('longitude', 0.0),
        "geography_altitude": data['geography'].get('altitude', 0.0),
        "geography_direction": str(data['geography'].get('direction', '')),
        "speed_horizontal": data['speed'].get('horizontal', 0.0),
        "speed_vertical": data['speed'].get('vertical', 0.0),
        "departure_iataCode": data['departure'].get('iataCode', ''),
        "departure_icaoCode": data['departure'].get('icaoCode', ''),
        "arrival_iataCode": data['arrival'].get('iataCode', ''),
        "arrival_icaoCode": data['arrival'].get('icaoCode', ''),
        "aircraft_regNumber": data['aircraft'].get('regNumber', ''),
        "aircraft_icaoCode": data['aircraft'].get('icaoCode', ''),
        "aircraft_icao24": data['aircraft'].get('icao24', ''),
        "airline_iataCode": data['airline'].get('iataCode', ''),
        "airline_icaoCode": data['airline'].get('icaoCode', ''),
        "flight_iataNumber": data['flight'].get('iataNumber', ''),
        "flight_icaoNumber": data['flight'].get('icaoNumber', ''),
        "flight_number": data['flight'].get('number', ''),
        "system_updated": data['system'].get('updated', 0),
        "system_squawk": str(data['system'].get('squawk', '')),
        "system_status": data['system'].get('status', ''),
    }
    print(f"Sending message: {message}")
    avro_producer.produce(topic=topic, value=message)
    avro_producer.flush()

def fetch_and_send_flight_data():
    api_call_count = 0
    while api_call_count < 10:
        try:
            response = requests.get('https://aviation-edge.com/v2/public/flights?key=768627-2b2401&limit=10000')
            data = response.json()
            for flight in data:
                send_to_kafka('AviationEdgeFlightTracker', flight)
            print(f"API call count: {api_call_count+1}")
        except Exception as e:
            print(f"An error occurred: {e}")
        api_call_count += 1
        time.sleep(30)

fetch_and_send_flight_data()


