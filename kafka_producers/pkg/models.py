"""
Insight Data Engineering
Kyle Schmidt

Base Model that contains table setting applicable to all tables
"""
from datetime import datetime

from sqlalchemy import Column, Integer, DateTime, String, ForeignKey, Table, MetaData, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.schema import UniqueConstraint

from pkg.config_secure import MYSQL_CONN

engine = create_engine(MYSQL_CONN)
meta = MetaData(bind=engine, reflect=True)


# class Tags(meta):
#     __tablename__ = "tags"
#     id = Column(Integer,
#                 autoincrement=True,
#                 nullable=False,
#                 primary_key=True)
#     created_time = Column(DateTime,
#                           default=datetime.now)
#     updated_time = Column(DateTime,
#                           default=datetime.now,
#                           onupdate=datetime.now)
#     tag = Column(String(50),
#                  index=True,
#                  unique=True,
#                  nullable=False)


# class MatchPhotosTags(meta):

#     __tablename__ = "match_photos_tags"
#     __table_args__ = (UniqueConstraint("photo_id", "tag_id", name="uix_1"),)
#     id = Column(Integer,
#                 autoincrement=True,
#                 nullable=False,
#                 primary_key=True)
#     created_time = Column(DateTime,
#                           default=datetime.now)
#     updated_time = Column(DateTime,
#                           default=datetime.now,
#                           onupdate=datetime.now)
#     photo_id = Column(Integer,
#                       ForeignKey("photos.id"),
#                       index=True,
#                       nullable=False)
#     tag_id = Column(Integer,
#                     ForeignKey("tags.id"),
#                     index=True,
#                     nullable=False)


# class Likes(meta):

#     __tablename__ = "likes"
#     id = Column(Integer,
#                 autoincrement=True,
#                 nullable=False,
#                 primary_key=True)
#     created_time = Column(DateTime,
#                           default=datetime.now)
#     updated_time = Column(DateTime,
#                           default=datetime.now,
#                           onupdate=datetime.now)
#     photo_id = Column(Integer,
#                       ForeignKey("photos.id"),
#                       nullable=False)
#     user_id = Column(Integer,
#                      ForeignKey("users.id"),
#                      nullable=False)


# followers = Table(
#     "followers", meta,
#     Column('followee', Integer, ForeignKey('users.id'),
#            primary_key=True),
#     Column('follower', Integer, ForeignKey('users.id'),
#            primary_key=True),
#     Column('id', Integer),
#     Column('created_time', DateTime),
#     Column('updated_time', DateTime))


# class Comments(meta):

#     __tablename__ = "comments"
#     id = Column(Integer,
#                 autoincrement=True,
#                 nullable=False,
#                 primary_key=True)
#     created_time = Column(DateTime,
#                           default=datetime.now)
#     updated_time = Column(DateTime,
#                           default=datetime.now,
#                           onupdate=datetime.now)
#     comment = Column(String(50),
#                      nullable=False)
#     user_id = Column(Integer(),
#                      ForeignKey("users.id"),
#                      nullable=False)
#     photo_id = Column(Integer(),
#                       ForeignKey("photos.id"),
#                       nullable=False)


# class Photos(meta):

#     __tablename__ = "photos"
#     id = Column(Integer,
#                 autoincrement=True,
#                 nullable=False,
#                 primary_key=True)
#     created_time = Column(DateTime,
#                           default=datetime.now)
#     updated_time = Column(DateTime,
#                           default=datetime.now,
#                           onupdate=datetime.now)
#     link = Column(String(500),
#                   index=True,
#                   unique=True,
#                   nullable=False)
#     location = Column(String(500),
#                       nullable=False)
#     user_id = Column(Integer,
#                      ForeignKey("users.id"),
#                      nullable=False)
#     tags = relationship("Tags",
#                         secondary="match_photos_tags",
#                         backref="",
#                         cascade="all")
#     comments = relationship("Comments",
#                             backref="photos",
#                             cascade="all")
#     likes = relationship("Users",
#                          secondary="likes",
#                          cascade="all")


# class Users(meta):

#     __tablename__ = "users"
#     id = Column(Integer,
#                 autoincrement=True,
#                 nullable=False,
#                 primary_key=True)
#     created_time = Column(DateTime,
#                           default=datetime.now)
#     updated_time = Column(DateTime,
#                           default=datetime.now,
#                           onupdate=datetime.now)
#     username = Column(String(50),
#                       index=True,
#                       unique=True,
#                       nullable=False)
#     full_name = Column(String(50),
#                        nullable=False)
#     last_login = Column(DateTime,
#                         index=True,
#                         default=datetime.now,
#                         onupdate=datetime.now)
#     comments = relationship("Comments",
#                             backref="users",
#                             cascade="all")
#     photos = relationship("Photos",
#                           backref="users",
#                           cascade="all")
#     likes = relationship("Likes",
#                          backref="users",
#                          cascade="all")
#     followers = relationship("Users",
#                              secondary=followers,
#                              primaryjoin="Users.id==Followers.followee",
#                              secondaryjoin="Users.id==Followers.follower",
#                              cascade="all")
