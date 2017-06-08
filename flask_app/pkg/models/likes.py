"""
Insight Data Engineering
Kyle Schmidt

Likes Table
"""
from .. import db
from .model_base import Base


class Likes(Base):

    __tablename__ = "likes"

    photo_id = db.Column(db.Integer(),
                         db.ForeignKey("photos.id"),
                         nullable=False)
    user_id = db.Column(db.Integer(),
                        db.ForeignKey("users.id"),
                        nullable=False)
