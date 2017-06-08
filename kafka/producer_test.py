"""
Insight Data Engineering
Kyle Schmidt

Kafka Producer Test Script
"""
from datetime import datetime
import json
import random
import re
import time
from typing import Generator, List, Tuple

from faker import Faker
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer

import config_secure


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


def gen_fake_users(num_fakes: int) -> Generator[Tuple[str, str], None, None]:
    """
    Generate Fake Users

    Arguments:
        num_fakes: Number of fake users to generate
    """
    fake = Faker()
    for _ in range(num_fakes):
        full_name = fake.name()  # pylint: disable=no-member
        name = remove_non_alpha_chars(full_name).lower()
        yield name, full_name


def main(servers: List[str],
         num_fakes: int = 1000000):
    """
    Main Method
    """
    simple_client = SimpleClient(servers)
    create_user_producer = KeyedProducer(simple_client)

    for username, full_name in gen_fake_users(num_fakes):
        record = {
            "username": username,
            "full_name": full_name
        }
        create_user_producer.send_messages('create-user',
                                           bytes(username, 'utf-8'),
                                           json.dumps(record).encode('ascii'))
        time.sleep(1)


if __name__ == '__main__':
    main(config_secure.SERVERS)
