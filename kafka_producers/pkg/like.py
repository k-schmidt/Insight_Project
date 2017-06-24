"""
Insight Data Engineering
Kyle Schmidt

Like Kafka Producer
"""
import json
import random
from typing import Dict, List, Optional, Tuple

from kafka.producer import KeyedProducer

from helper_functions import get_datetime


def like_producer(users: List[Tuple[str]],
                  photos: List[Tuple[str, str]],
                  tags: List[Tuple[str]],
                  locations: List[Tuple[str, str]],
                  producer: KeyedProducer) -> Optional[Dict[str, str]]:
    """
    Produce like events to Kafka

    Arguments:
        users: List of users who can produce an event
        photos: Queue of recent photos and their usernames
        tags: List of company names
        locations: List of possible global lat/long coordinates
        producer: Kafka producer object to post messages

    Returns:
        Kafka message
    """
    if not photos:
        return
    follower = random.choice(users)[0]
    photo, followee = random.choice(photos)
    created_time, partition_date = get_datetime()
    record = {
        "follower_username": follower,
        "followed_username": followee,
        "photo_id": photo,
        "created_time": created_time,
        "partition_date": partition_date,
        "event": "like"
    }
    producer.send_messages("like",
                           bytes(followee, 'utf-8'),
                           json.dumps(record).encode('utf-8'))
    return record
