"""
Insight Data Engineering
Kyle Schmidt

Followers Table
"""
from datetime import datetime

from sqlalchemy.schema import UniqueConstraint

from .. import db
from .model_base import Base


class MatchPhotosTags(Base):

    __tablename__ = "match_photos_tags"
    __table_args__ = (UniqueConstraint("photo_id", "tag_id", name="uix_1"),)

    photo_id = db.Column(db.Integer(),
                         db.ForeignKey("photos.id"),
                         index=True,
                         nullable=False)
    tag_id = db.Column(db.Integer(),
                       db.ForeignKey("tags.id"),
                       index=True,
                       nullable=False)
