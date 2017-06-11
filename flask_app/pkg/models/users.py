"""
Insight Data Engineering
Kyle Schmidt

Users Table
"""
from datetime import datetime

from .. import db
from .followers import Followers
from .model_base import Base


class Users(Base):

    __tablename__ = "users"

    username = db.Column(db.String(50),
                         index=True,
                         unique=True,
                         nullable=False)
    full_name = db.Column(db.String(50),
                          nullable=False)
    last_login = db.Column(db.DateTime(),
                           index=True,
                           default=datetime.now,
                           onupdate=datetime.now)
    comments = db.relationship("Comments",
                               backref="users",
                               cascade="all")
    photos = db.relationship("Photos",
                             backref="users",
                             cascade="all")
    likes = db.relationship("Likes",
                            backref="users",
                            cascade="all")
    followers = db.relationship("Users",
                                secondary="followers",
                                primaryjoin="Users.id==Followers.followee",
                                secondaryjoin="Users.id==Followers.follower",
                                backref="users",
                                cascade="all")
