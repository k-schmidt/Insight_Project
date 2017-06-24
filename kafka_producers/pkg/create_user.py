"""
Insight Data Engineering
Kyle Schmidt

Create User Kafka Producer
"""
from datetime import datetime
import json
import random
import re
import time
from typing import Tuple
import uuid

from faker import Faker


def get_datetime():
    datetime_obj = datetime.now()
    return datetime_obj.strftime("%Y-%m-%d %H:%M:%S"), datetime_obj.strftime("%Y-%m-%d")


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
    username = name + "{:03d}".format(random.randrange(1, 999))
    return username, full_name


def create_user_producer(servers, users, photos, tags, locations, producer):
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
