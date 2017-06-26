"""
Insight Data Engineering
Kyle Schmidt

Follow Kafka Producer
"""
import json
import random
from typing import Deque, Dict, List, Tuple

from kafka.producer import KeyedProducer

from pkg.helper_functions import get_datetime


def follow_producer(users: List[Tuple[str]],
                    photos: Deque[Tuple[str, str]],
                    tags: List[Tuple[str]],
                    locations: List[Tuple[str, str]],
                    producer: KeyedProducer) -> Dict[str, str]:
    """
    Produce follow events to Kafka

    Arguments:
        users: List of users who can produce an event
        photos: Queue of recent photos and their usernames
        tags: List of company names
        locations: List of possible global lat/long coordinates
        producer: Kafka producer object to post messages

    Returns:
        Kafka message
    """
    followee, follower = random.choice(users)[0], random.choice(users)[0]
    created_time, parition_date = get_datetime()
    record = {
        "follower_username": follower,
        "followed_username": followee,
        "created_time": created_time,
        "partition_date": parition_date,
        "event": "follow"
    }
    producer.send_messages("follow",
                           bytes(followee, 'utf-8'),
                           json.dumps(record).encode('utf-8'))
    return record
