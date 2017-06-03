"""
Generating Social Network
Kyle Schmidt

Creating MySQL models to help with relationships
"""
from datetime import datetime
from typing import Any

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Table
from sqlalchemy.ext.declarative import declarative_base  # type: ignore
from sqlalchemy.orm import relationship  # type: ignore

ModelBase: Any = declarative_base()  # pylint: disable=invalid-name

follower_relationship = Table(  # pylint: disable=invalid-name
    "relationships", ModelBase.metadata,
    Column('followee', Integer, ForeignKey('users.id'),
           primary_key=True),
    Column('follower', Integer, ForeignKey('users.id'),
           primary_key=True))


class User(ModelBase):  # pylint: disable=too-few-public-methods
    """
    MySQL User Table
    """
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)  # pylint: disable=invalid-name
    created_time = Column(DateTime, default=datetime.now)
    name = Column(String(500))
    full_name = Column(String(500))

    followers = relationship("User",
                             secondary=follower_relationship,
                             primaryjoin=id == follower_relationship.c.followee,
                             secondaryjoin=id == follower_relationship.c.follower,)
