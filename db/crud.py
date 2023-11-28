from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.expression import func
from sqlalchemy.exc import NoResultFound
from sqlalchemy import select

from . import models


# async def create_order(db: AsyncSession, username: str, token_name: str, amount:int):
#     db_order = models.Order(username=username, token_name=token_name, amount=amount)
#     db.add(db_order)
#     db.commit()
#     db.refresh(db_order)
#     return db_order


async def create_order(db: AsyncSession, username: str, token_name: str, amount: int):
    db_order = models.Order(username=username, token_name=token_name, amount=amount, status="Processing")
    async with db.begin():
        db.add(db_order)
        await db.flush()
    return db_order


async def get_all_orders(db: AsyncSession):
    statement = select(models.Order)
    result = await db.execute(statement)
    return result.scalars().all()