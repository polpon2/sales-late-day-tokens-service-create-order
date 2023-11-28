import asyncio, aio_pika, json
from db.engine import SessionLocal, engine
from db import crud, models

async def process_message(
    message: aio_pika.abc.AbstractIncomingMessage,
    connection: aio_pika.Connection,  # Add connection parameter
) -> None:
    async with message.process():
        body: dict = json.loads(message.body)

        username: str = body['username']
        token_name: str = body['token_name']
        amount: int = body['amount']

        print(f" [x] Received {body}")

        # Create Order.
        async with SessionLocal() as db:
            await crud.create_order(db=db, username=username, token_name=token_name, amount=amount)
            print(f"create order")

        routing_key = "from.order"

        channel = await connection.channel()

        await channel.default_exchange.publish(
            aio_pika.Message(body=message.body),
            routing_key=routing_key,
        )


async def main() -> None:
    connection = await aio_pika.connect_robust(
        "amqp://localhost",
    )

    # Init the tables in db
    async with engine.begin() as conn:
        await conn.run_sync(models.Base.metadata.drop_all) # Reset every time
        await conn.run_sync(models.Base.metadata.create_all)


    queue_name = "to.order"

    # Creating channel
    channel = await connection.channel()

    # Maximum message count which will be processing at the same time.
    await channel.set_qos(prefetch_count=10)

    # Declaring queue
    queue = await channel.declare_queue(queue_name, arguments={
                                                    'x-message-ttl' : 1000,
                                                    'x-dead-letter-exchange' : 'dlx',
                                                    'x-dead-letter-routing-key' : 'dl'
                                                    })

    print(' [*] Waiting for messages. To exit press CTRL+C')

    await queue.consume(lambda message: process_message(message, connection))

    try:
        # Wait until terminate
        await asyncio.Future()
    finally:
        await connection.close()


if __name__ == "__main__":
    asyncio.run(main())