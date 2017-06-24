"""
Insight Data Engineering
Kyle Schmidt

Create User Kafka Producer
"""
import json
import random
import re
from typing import Dict, List, Tuple

from faker import Faker
from kafka.producer import KeyedProducer

from helper_functions import get_datetime


def remove_non_alpha_chars(string: str) -> str:
    """
    Remove non-alphabetical characters from given string

    Arguments:
        string: String to replace chars

    Returns:
        string without non-alphabetical characters
    """
    regex = re.compile('[^a-zA-Z]')
    return regex.sub('', string)


def fake_user() -> Tuple[str, str]:
    """
    Generate Fake Users

    Arguments:
        num_fakes: Number of fake users to generate

    Returns:
        Tuple of username and their full name
        Username has a random 3 digit number appended to their name
    """
    fake = Faker()
    full_name = fake.name()  # pylint: disable=no-member
    name = remove_non_alpha_chars(full_name).lower()
    username = name + "{:03d}".format(random.randrange(1, 999))
    return username, full_name


def create_user_producer(users: List[Tuple[str]],
                         photos: List[Tuple[str, str]],
                         tags: List[Tuple[str]],
                         locations: List[Tuple[str, str]],
                         producer: KeyedProducer) -> Dict[str, str]:
    """
    Produce create-user events to Kafka

    Arguments:
        users: List of users who can produce an event
        photos: Queue of recent photos and their usernames
        tags: List of company names
        locations: List of possible global lat/long coordinates
        producer: Kafka producer object to post messages

    Returns:
        Kafka message
    """
    username, full_name = fake_user()
    created_time, partition_date = get_datetime()

    record = {
        "username": username,
        "full_name": full_name,
        "created_time": created_time,
        "partition_date": partition_date,
        "event": "create-user"
    }
    producer.send_messages("create-user",
                           bytes(username, 'utf-8'),
                           json.dumps(record).encode('utf-8'))
    users.append((username,))
    return record
