# producer.py

#########################################################
#                                                       #
#  DON'T CHANGE ANYTHING HERE UNTIL LINE 190            #
#                                                       #
#########################################################

# Standard library imports for logging, operating system interactions, and type annotations
import logging
import os
from typing import List

# FastAPI framework for building APIs and handling HTTP requests and responses
from fastapi import FastAPI, HTTPException, status

# dotenv for loading environment variables from a .env file for configuration
from dotenv import load_dotenv

# Confluent Kafka client libraries for producing messages and managing Kafka topics
from confluent_kafka.admin import AdminClient, NewTopic  # Kafka administration (topics management)
from confluent_kafka import SerializingProducer  # Kafka producer with serialization capabilities
from confluent_kafka.serialization import StringSerializer  # Serializer for message keys
from confluent_kafka.schema_registry import SchemaRegistryClient  # For schema registry interactions
from confluent_kafka.schema_registry.avro import AvroSerializer  # Avro serializer for message values

# Libraries for generating fake data, compressing data, and creating unique identifiers
from faker import Faker  # Library to generate fake data (e.g., names, addresses)
import zlib  # Compression library for generating shorter unique IDs using CRC32
import uuid  # Library for generating unique identifiers

# Local modules containing application-specific classes and schemas
import schemas  # Avro schema definitions for Kafka message serialization
from entities import TripStreamModel, DriverEarningStreamModel, RiderStreamModel
from commands import CreateTripCommand, CreateDriverEarningCommand, CreateRiderCommand
from commands import GenerateMultipleTripsCommand


# Generate some random completed trip records. 
## Purely simulation. 
from random import uniform, choice
from datetime import datetime, timedelta
import pytz  # For timezone handling

faker_instance = Faker()

def get_time_components(iso_timestamp):
    timestamp = datetime.fromisoformat(iso_timestamp)
    return {
        "date": timestamp.date(),
        "day_of_week": timestamp.strftime('%A'),
        "hour": timestamp.hour,
        "minute": timestamp.minute
    }


# Centralized function for generating a unique trip ID
def generate_unique_trip_id():
    return faker_instance.uuid4()

def generate_trip_id():
    return faker_instance.uuid4()

def generate_driver_id():
    return faker_instance.uuid4()

def generate_rider_id():
    return faker_instance.uuid4()


# Constants
X_PER_MILE = 2.00  # $2.00 per mile
FARE_PER_MINUTE = 0.50  # $0.50 per minute

import numpy as np
from random import random

def generate_realistic_mileage():
    # Probabilities for each type of trip
    long_commute_probability = 0.1  # 10% chance of being a long commute
    mid_range_probability = 0.3     # 30% chance of being a mid-range trip
    # Remaining probability will be for short trips

    # Short trip distribution parameters (log-normal)
    mean_short = 2
    sigma_short = 0.7

    # Mid-range trip distribution parameters (normal)
    mean_mid = 10
    sigma_mid = 5

    # Long trip distribution parameters (uniform)
    min_long_mileage = 15
    max_long_mileage = 65

    random_value = random()
    if random_value < long_commute_probability:
        # Generate a long trip
        mileage = np.random.uniform(min_long_mileage, max_long_mileage)
    elif random_value < long_commute_probability + mid_range_probability:
        # Generate a mid-range trip
        mileage = np.random.normal(mean_mid, sigma_mid)
    else:
        # Generate a short trip
        mileage = np.random.lognormal(mean_short, sigma_short)

    # Ensure mileage is within a realistic range (1 to max_long_mileage miles)
    mileage = max(1, min(max_long_mileage, mileage))

    return round(mileage, 2)


def generate_realistic_duration(mileage):
    # Assuming each mile takes between 1.0 to 2.5 minutes
    min_time_per_mile = 1.0
    max_time_per_mile = 2.5

    duration = mileage * uniform(min_time_per_mile, max_time_per_mile)
    
    return round(duration, 2)  # Round to 2 decimal places for realism

def generate_fake_trip(trip_id, driver_id):
    mileage = generate_realistic_mileage()
    duration = generate_realistic_duration(mileage)

    return TripStreamModel(
        trip_id=trip_id,
        driver_id=driver_id,
        duration=f"{round(duration)} minutes",
        mileage=f"{round(mileage)} miles",
        pickup_location=faker_instance.address(),
        destination_location=faker_instance.address()
        # start_time=str(datetime.now()),
        # completion_time=str(datetime.now())
    )

def generate_fake_rider(trip_id, rider_id):
    # Generate estimated and actual durations
    actual_duration = int(faker_instance.random_int(min=15, max=60))
    duration_estimate = actual_duration + round(uniform(-5, 5))  # Within 5 mins 95% of times, 10 mins 99% of times

    # Calculate fare based on duration
    initial_fare_estimate = duration_estimate * FARE_PER_MINUTE + uniform(-1, 1)
    final_adjusted_fare = actual_duration * FARE_PER_MINUTE + uniform(-1, 1)

    # Payment status
    payment_status = "Completed" if uniform(0, 1) < 0.99 else "Refunded"

    # Driver rating
    if duration_estimate >= actual_duration:
        rating_to_driver = choice([4, 5])
    else:
        delay = actual_duration - duration_estimate
        rating_to_driver = 5 - min(4, delay)  # 1 to 3 depending on the delay

    # Generating timestamps
    completion_time = datetime.now(pytz.utc)  # Current time as completion time
    start_time = completion_time - timedelta(minutes=actual_duration)  # Subtract actual duration to get start time

    return RiderStreamModel(
        rider_id=rider_id,
        trip_id=trip_id,
        duration_estimate=f"{duration_estimate} minutes",
        initial_fare_estimate=initial_fare_estimate,
        final_adjusted_fare=final_adjusted_fare,
        payment_status=payment_status,
        rating_to_driver=rating_to_driver
    )


def generate_fake_driver_earning(trip_id, driver_id, mileage, duration):
    # Earnings based on mileage and duration
    earnings_from_trip = mileage * X_PER_MILE * 0.8 + duration * FARE_PER_MINUTE * 0.2

    return DriverEarningStreamModel(
        driver_id=driver_id,
        trip_id=trip_id,
        earnings_from_trip=earnings_from_trip
    )

# standard module for logging messages
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# loading secrets to connect confluent kafka
load_dotenv(verbose=True)






#########################################################
#                                                       #
#       YOUR HOMEWORK BEGINS HERE (After Line 190)      #
#                                                       #
#########################################################

app = FastAPI()

# 1. Kafka Configuration
def get_kafka_config():
    """
    Load Kafka configuration from environment variables.
    HINT: Use os.getenv to load environment variables for Kafka configuration.
    """
    return {
        "bootstrap.servers": os.environ['BOOTSTRAP_SERVERS'],
        # TODO: Add other configurations here (e.g., security settings)
        'security.protocol': os.environ['SECURITY_PROTOCOL'],
        'sasl.mechanisms': os.environ['SASL_MECHANISMS'],
        'sasl.username': os.environ['SASL_USERNAME'],
        'sasl.password': os.environ['SASL_PASSWORD']
    }


@app.on_event("startup")
async def startup_event():
    """
    Create Kafka topics on application startup.
    HINT: Use AdminClient and NewTopic to create Kafka topics.
    """
    admin_client = AdminClient(get_kafka_config())
    # topics = [NewTopic(topic=os.getenv("TOPIC_NAME"), num_partitions=3, replication_factor=1)]
    # TODO: Create topics (handle potential exceptions)
    topics_to_create = [
        NewTopic(
            os.environ['TOPICS_TRIPS'],
            num_partitions=int(os.environ['TOPIC_TRIPS_PARTITIONS']),
            replication_factor=int(os.environ['TOPIC_TRIPS_REPLICAS'])
        ),
        NewTopic(
            os.environ['TOPICS_RIDES'],
            num_partitions=int(os.environ['TOPIC_RIDES_PARTITIONS']),
            replication_factor=int(os.environ['TOPIC_RIDES_REPLICAS'])
        ),
        NewTopic(
            os.environ['TOPICS_DRIVERS'],
            num_partitions=int(os.environ['TOPIC_DRIVERS_PARTITIONS']),
            replication_factor=int(os.environ['TOPIC_DRIVERS_REPLICAS'])
        ),
    ]
    try:
        futures = admin_client.create_topics(topics_to_create)
        for topic, future in futures.items():
            future.result()
            logger.info(f'created topic {topic}')
    except Exception as e:
        logger.warning(f'An error occurred while creating topics: {e}')


# 2. Unique Request ID Generation
def generate_request_id():
    """
    Generate a unique request ID using UUID and zlib.
    HINT: Use uuid and zlib to generate a unique request ID.
    """
    unique_id = str(uuid.uuid4())
    crc32_id = zlib.crc32(unique_id.encode())
    return f"{unique_id}-{crc32_id}"

# 3. Kafka Producer Setup
def make_producer(schema_str: str) -> SerializingProducer:
    """
    Create and return a Kafka producer with Avro serialization.
    HINT: Use AvroSerializer for value_serializer.
    """
    schema_registry_client = SchemaRegistryClient({"url": os.getenv("SCHEMA_REGISTRY_URL"),
                                                    'basic.auth.user.info': os.environ['basic_auth.user_info']
                                                })
    value_serializer = AvroSerializer(schema_str=schema_str, 
                                      schema_registry_client=schema_registry_client,
                                      to_dict=lambda val, ctx: val.dict(by_alias=True))
    return SerializingProducer({
        **get_kafka_config(),
        "key.serializer": StringSerializer(),
        "value.serializer": value_serializer,
    })

# 4. Producer Callback
class ProducerCallback:
    """
    Callback class for Kafka producer to handle message delivery reports.
    """
    def __call__(self, err, msg):
        if err is not None:
            logger.error(f"Delivery failed for record: {err}")
        else:
            logger.info(f"Record delivered to {msg.topic()}")

# 5. FastAPI Endpoints Implementation
@app.post("/api/generate-trips")
async def generate_trip():
    producer = make_producer(schema_str=schemas.trip_stream_schema)

    trip_id = generate_unique_trip_id()
    driver_id = generate_driver_id()

    trip_data = generate_fake_trip(trip_id, driver_id)
    # print(f'---------------{trip_data}-------------------')
    producer.produce(
        topic=os.environ["TOPICS_TRIPS"],
        key=trip_id,
        value=trip_data,
        on_delivery=ProducerCallback()
    )

    producer.flush()
    return trip_data


# TODO: Implement other endpoints following the same pattern
@app.post("/api/generate-riders")
async def generate_trip(command:CreateRiderCommand):
    producer = make_producer(schema_str=schemas.rider_stream_schema)

    trip_id = generate_unique_trip_id()
    rider_id = generate_rider_id()
    rider_data = generate_fake_trip(trip_id, rider_id)

    producer.produce(
        topic=os.getenv("TOPICS_RIDES"),
        key=str(rider_id),
        value=rider_data,
        on_delivery=ProducerCallback()
    )

    producer.flush()
    return rider_data

@app.post("/api/generate-complete-trips")
async def generate_complete_trip(command:GenerateMultipleTripsCommand):
    trip_id = generate_unique_trip_id()
    driver_id = generate_driver_id()
    rider_id = generate_rider_id()
    
    trip_data = generate_fake_trip(trip_id, driver_id)
    mileage = int(trip_data.mileage.split()[0]) # Extract mileage as integer
    duration = int(trip_data.duration.split()[0]) # Extract duration as integer

    driver_earning_data = generate_fake_driver_earning(trip_id, driver_id, mileage, duration)

    rider_data = generate_fake_rider(trip_id, rider_id)
    
    # Producer logic for each stream
    trip_producer = make_producer(schema_str=schemas.trip_stream_schema)
    driver_producer = make_producer(schema_str=schemas.driver_earning_stream_schema)
    rider_producer = make_producer(schema_str=schemas.rider_stream_schema)

    # Producer logic for each stream (send messages)
    trip_producer.produce(
        topic=os.getenv("TOPICS_TRIPS"),
        key=str(trip_id),
        value=trip_data,
        on_delivery=ProducerCallback()
    )

    driver_producer.produce(
        topic=os.getenv("TOPICS_DRIVERS"),
        key=str(driver_id),
        value=driver_earning_data,
        on_delivery=ProducerCallback()
    )

    rider_producer.produce(
        topic=os.getenv("TOPICS_RIDES"),
        key=str(rider_id),
        value=rider_data,
        on_delivery=ProducerCallback()
    )

    trip_producer.flush()
    driver_producer.flush()
    rider_producer.flush()

    return{
        "trip_id": trip_id,
        "trip_data": trip_data,
        "driver_earning_data": driver_earning_data,
        "rider_data": rider_data
    }

@app.post("/api/generate-multiple-trips")
async def generate_trip(command:GenerateMultipleTripsCommand):
    """
    Endpoint to generate a trip.
    HINT: Use the make_producer function to create a producer and send a message to the Kafka topic.
    """
    # TODO: Implement the logic to generate a trip and send it to the Kafka topic

    trip_producer = make_producer(schema_str=schemas.trip_stream_schema)
    driver_producer = make_producer(schema_str=schemas.driver_earning_stream_schema)
    rider_producer = make_producer(schema_str=schemas.rider_stream_schema)

    all_trip_data = []driver_earning_data = generate_fake_driver_earning(trip_id, driver_id, mileage, duration)

        rider_data = generate_fake_rider(trip_id, rider_id)

        # Producer logic for each stream (send messages)
        trip_producer.produce(
            topic=os.getenv("TOPICS_TRIPS"),
            key=str(trip_id),
            value=trip_data,
            on_delivery=ProducerCallback()
        )

      
    for _ in range(command.number_of_trips):
        trip_id = generate_unique_trip_id()
        driver_id = generate_driver_id()
        rider_id = generate_rider_id()
        
        trip_data = generate_fake_trip(trip_id, driver_id)
        mileage = int(trip_data.mileage.split()[0]) # Extract mileage as integer
        duration = int(trip_data.duration.split()[0]) # Extract duration as integer

          driver_producer.produce(
            topic=os.getenv("TOPICS_DRIVERS"),
            key=str(driver_id),
            value=driver_earning_data,
            on_delivery=ProducerCallback()
        )

        rider_producer.produce(
            topic=os.getenv("TOPICS_RIDES"),
            key=str(rider_id),

           ≥ 
            value=rider_data,
            on_delivery=ProducerCallback()
        )

        trip_producer.flush()
        driver_producer.flush()
        rider_producer.flush()

        all_trip_data.append({
                "trip_id": trip_id,
                "trip_data": trip_data,
                "driver_earning_data": driver_earning_data,
                "rider_data": rider_data
            })

    return all_trip_data


# [Uncomment the following line when ready to run the server]
# uvicorn producer:app --reload --port 8001