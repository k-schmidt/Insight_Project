"""
Insight Data Engineering
Kyle Schmidt

Users Table
"""
from .. import db
from .model_base import Base


class Photos(Base):

    __tablename__ = "photos"

    link = db.Column(db.String(500),
                     index=True,
                     unique=True,
                     nullable=False)
    location = db.Column(db.String(500),
                         nullable=False)
    user_id = db.Column(db.Integer(),
                        db.ForeignKey("users.id"),
                        nullable=False)
    tags = db.relationship("Tags",
                           secondary="match_photos_tags",
                           backref="",
                           cascade="all")
    comments = db.relationship("Comments",
                               backref="photos",
                               cascade="all")
    likes = db.relationship("Users",
                            secondary="likes",
                            cascade="all")
