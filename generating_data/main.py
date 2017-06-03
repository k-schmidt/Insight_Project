"""
Insight Data Engineering
Kyle Schmidt

Main method to generate user data
"""
import random
import re
from typing import Generator

from faker import Faker  # type: ignore
import sqlalchemy  # type: ignore
from sqlalchemy import create_engine
from sqlalchemy.orm.session import Session  # type: ignore

from pkg import models
from config_secure import MYSQL_ENGINE


def mysql_connection(mysql_engine: str) -> Session:
    """
    Create MySQL Connection

    Arguments:
        mysql_engine: URI of mysql database

    Returns:
        mysql session
    """
    engine = create_engine(mysql_engine)
    models.ModelBase.metadata.create_all(engine)
    SessionMaker = sqlalchemy.orm.sessionmaker(bind=engine)  # pylint: disable=invalid-name
    session = SessionMaker()
    return session


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


def gen_fake_users(num_fakes: int) -> Generator[models.User, None, None]:
    """
    Generate Fake Users

    Arguments:
        num_fakes: Number of fake users to generate
    """
    fake = Faker()
    for _ in range(num_fakes):
        full_name = fake.name()  # pylint: disable=no-member
        name = remove_non_alpha_chars(full_name)
        user = models.User(name=name, full_name=full_name)
        yield user


def main(mysql_engine: str = MYSQL_ENGINE,
         num_fakes: int = 1000000,
         max_followers: int = 1000):
    """
    Main method
    """
    session = mysql_connection(mysql_engine)
    for user in gen_fake_users(num_fakes):
        session.add(user)
    session.commit()

    for user in session.query(models.User).all():
        for _ in range(random.randrange(max_followers)):
            follower = session.query(models.User)\
                              .filter(models.User.id == random.randrange(1, num_fakes+1)).first()
            if follower != user:
                user.followers.append(follower)
        session.add(user)
    session.commit()

if __name__ == '__main__':
    main()
