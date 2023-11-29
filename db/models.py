from sqlalchemy import Boolean, Column, DateTime, ForeignKey, ForeignKeyConstraint, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from .engine import Base

class Order(Base):
    __tablename__ = "orders"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(255))
    amount = Column(Integer)
    status = Column(String(255), default="UNKNOWN");

    def __repr__(self):
        return f"<Order(username={self.username}, amount={self.amount}, status={self.status})>"


