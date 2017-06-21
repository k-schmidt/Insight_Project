"""
Insight Data Engineering
Kyle Schmidt

Tags Table
"""
from .. import db
from .model_base import Base


class Tags(Base):

    __tablename__ = "tags"

    tag = db.Column(db.String(50),
                    index=True,
                    unique=True,
                    nullable=False)
