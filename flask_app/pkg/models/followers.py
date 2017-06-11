"""
Insight Data Engineering
Kyle Schmidt

Followers Table
"""
from datetime import datetime

from sqlalchemy.schema import UniqueConstraint

from .. import db
from .model_base import Base


class Followers(Base):

    __tablename__ = "followers"
    __table_args__ = (UniqueConstraint("followee", "follower", name="uix_1"),)

    followee = db.Column(db.Integer(),
                         db.ForeignKey("users.id"),
                         index=True,
                         nullable=False)
    follower = db.Column(db.Integer(),
                         db.ForeignKey("users.id"),
                         index=True,
                         nullable=False)
