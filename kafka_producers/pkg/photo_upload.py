"""
Insight Data Engineering
Kyle Schmidt

Photo Upload Kafka Producer
"""
import json
import random
from typing import Dict, List, Tuple

from kafka.producer import KeyedProducer

from helper_functions import get_datetime


def create_photo_producer(users: List[Tuple[str]],
                          photos: List[Tuple[str, str]],
                          tags: List[Tuple[str]],
                          locations: List[Tuple[str, str]],
                          producer: KeyedProducer) -> Dict[str, str]:
    """
    Produce photo-upload events to Kafka

    Arguments:
        users: List of users who can produce an event
        photos: Queue of recent photos and their usernames
        tags: List of company names
        locations: List of possible global lat/long coordinates
        producer: Kafka producer object to post messages

    Returns:
        Kafka message
    """
    user = random.choice(users)[0]
    tag, link = random.choice(tags)
    latitude, longitude = random.choice(locations)
    created_time, partition_date = get_datetime()
    record = {
        "username": user,
        "tags": tag,
        "photo_link": link,
        "created_time": created_time,
        "partition_date": partition_date,
        "latitude": latitude,
        "longitude": longitude,
        "event": "photo-upload"
    }
    producer.send_messages('photo-upload',
                           bytes(user, 'utf-8'),
                           json.dumps(record).encode('utf-8'))
    photos.append((created_time, user))
    return record
