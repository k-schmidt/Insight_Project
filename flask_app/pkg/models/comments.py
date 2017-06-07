"""
Insight Data Engineering
Kyle Schmidt

Comments Table
"""
from .. import db
from .model_base import Base


class Comments(Base):

    __tablename__ = "comments"

    comment = db.Column(db.String(50),
                        index=True,
                        nullable=False)
    user_id = db.Column(db.Integer(),
                        db.ForeignKey("users.id"),
                        nullable=False)
    photo_id = db.Column(db.Integer(),
                         db.ForeignKey("photos.id"),
                         nullable=False)
