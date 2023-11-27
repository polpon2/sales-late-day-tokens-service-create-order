from sqlalchemy.orm import Session
from sqlalchemy.sql.expression import func
from sqlalchemy.exc import NoResultFound

from . import models, schemas
from datetime import datetime

def create_order(db: Session, username: str, token_name: str, amount:int):
    db_order = models.Order(username=username, token_name=token_name, amount=amount)
    db.add(db_order)
    db.commit()
    db.refresh(db_order)
    return db_order