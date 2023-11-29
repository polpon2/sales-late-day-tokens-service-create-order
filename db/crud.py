from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.expression import func
from sqlalchemy.exc import NoResultFound
from sqlalchemy import select

from . import models


async def create_order(db: AsyncSession, username: str, amount: int):
    db_order = models.Order(username=username, amount=amount, status="Processing")
    db.add(db_order)
    await db.flush()
    return db_order


async def get_all_orders(db: AsyncSession):
    statement = select(models.Order)
    result = await db.execute(statement)
    return result.scalars().all()


async def change_status(db: AsyncSession, order_id: str, status: str = "UNKNOWN"):
    await db.execute(models.Order.__table__.update().where(models.Order.id == order_id).values({'status': status}))
    await db.flush()
    return True