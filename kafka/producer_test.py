"""
Insight Data Engineering
Kyle Schmidt

Kafka Producer Test Script
"""
from datetime import datetime
import json
import random
import re
from typing import Generator, Tuple

from faker import Faker
from kafka import KafkaProducer, SimpleProducer, SimpleClient

import config
import config_secure


# EVENTS = {
#     "photo": {
#         "username": random.choice(config.users),
#         "tags": random.choice(config.tags),
#         "link": random.randrange(100),
#         "created_time": datetime.now().strtime(config.datetime_format),
#         "location": random.choice(config.locations)
#     },
#     "comment": {
#         "text": ''.join(random.sample(string.ascii_letters, 15)),
#         "user": random.choice(config.users),
#         "photo": {
#             "user": random.choice(config.users),
#             "tags": random.choice(config.tags),
#             "link": random.randrange(100),
#             "created_time": datetime.now().strtime(config.datetime_format),
#             "location": random.choice(config.locations)
#         },
#         "created_time": datetime.now().strfitme(config.datetime_format)
#     },
#     "like": {
#         "username": random.choice(config.users),
#         "photo": {
#             "username": random.choice(config.users),
#             "tags": random.choice(config.tags),
#             "link": random.randrange(100),
#             "created_time": datetime.now().strtime(config.datetime_format),
#             "location": random.choice(config.locations)
#         },
#         "created_time": datetime.now().strtime(config.datetime_format)
#     },
#     "follow": {
#         "username": random.choice(config.users),
#         "followee": random.choice(config.users),
#         "created_time": datetime.now().strftime(config.datetime_format)
#     },
#     "unfollow": {
#         "username": random.choice(config.users),
#         "followee": random.choice(config.users),
#         "created_time": datetime.now().strtime(config.datetime_format)}
# }


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


def main(num_fakes: int = 100):
    """
    Main Method
    """
    json_producer = KafkaProducer(bootstrap_servers=config_secure.SERVERS,
                                  client_id="create-user",
                                  key_serializer=lambda m: bytes(m, 'utf-8'),
                                  value_serializer=lambda m: json.dumps(m).encode('ascii'))
    for username, full_name in gen_fake_users(num_fakes):
        print(username, full_name)
        record = {
            "username": username,
            "full_name": full_name,
            "created_time": datetime.now().strftime(config.datetime_format)
        }
        producer.send_messages("create-user",
                               bytes(record, "utf-8"),
                               key=username)


if __name__ == '__main__':
    main()
