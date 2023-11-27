from sqlalchemy.orm import Session
from sqlalchemy.sql.expression import func
from sqlalchemy.exc import NoResultFound

from . import models, schemas
from datetime import datetime

def create_order(db: Session, username: str, inventory_uuid: str, purchase_amount:int):
    db_order = models.Order(username=username, inventory_uuid=inventory_uuid, purchase_amount=purchase_amount, credits=100)
    db.add(db_order)
    db.commit()
    db.refresh(db_order)
    return db_order