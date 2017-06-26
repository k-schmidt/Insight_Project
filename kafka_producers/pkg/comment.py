"""
Insight Data Engineering
Kyle Schmidt

Comment Kafka Producer
"""
import json
import random
from typing import Deque, Dict, List, Optional, Tuple

from faker import Factory
from kafka.producer import KeyedProducer

from pkg.helper_functions import get_datetime


def get_text() -> str:
    """
    Generate random comments in latin

    Returns:
        Fake latin sentence
    """
    fake = Factory.create()
    return fake.sentence()


def comment_producer(users: List[Tuple[str]],
                     photos: Deque[Tuple[str, str]],
                     tags: List[Tuple[str]],
                     locations: List[Tuple[str, str]],
                     producer: KeyedProducer) -> Optional[Dict[str, str]]:
    """
    Produce comment events to Kafka

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
        return None
    follower = random.choice(users)[0]
    photo, followee = random.choice(photos)
    text = get_text()
    created_time, partition_date = get_datetime()

    if not all([photo, follower, followee]):
        return None
    record = {
        "follower_username": follower,
        "followed_username": followee,
        "photo_id": photo,
        "text": text,
        "created_time": created_time,
        "partition_date": partition_date,
        "event": "comment"
    }
    producer.send_messages("comment",
                           bytes(followee, 'utf-8'),
                           json.dumps(record).encode('utf-8'))
    return record
