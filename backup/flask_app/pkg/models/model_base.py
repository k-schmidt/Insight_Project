"""
Insight Data Engineering
Kyle Schmidt

Base Model that contains table setting applicable to all tables
"""
from datetime import datetime

from .. import db


class Base(db.Model):
    """
    Abstract Base SQLAlchemy Class
    """
    __abstract__ = True

    id = db.Column(db.Integer(),
                   autoincrement=True,
                   nullable=False,
                   primary_key=True)
    created_time = db.Column(db.DateTime(),
                             default=datetime.now)
    updated_time = db.Column(db.DateTime(),
                             default=datetime.now,
                             onupdate=datetime.now)
