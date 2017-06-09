"""
Insight Data Engineering
Kyle Schmidt

Create User Kafka Producer
"""
from datetime import datetime
import json
import re
import time
from typing import Tuple

from faker import Faker
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer


def get_datetime():
    return datetime.now().isoformat()


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
    """
    fake = Faker()
    full_name = fake.name()  # pylint: disable=no-member
    name = remove_non_alpha_chars(full_name).lower()
    return name, full_name


def create_user_producer(servers, Session):
    simple_client = SimpleClient(servers)
    producer = KeyedProducer(simple_client)
    username, full_name = fake_user()

    record = {
        "username": username,
        "full_name": full_name,
        "created_time": get_datetime(),
        "updated_time": get_datetime(),
        "last_login": get_datetime()
    }
    if not record: return
    producer.send_messages('create-user',
                           bytes(username, 'utf-8'),
                           json.dumps(record).encode('ascii'))
    return record
