import asyncio, aio_pika, json
from async_timeout import timeout
from asyncio import TimeoutError
from db.engine import SessionLocal, engine
from db import crud, models

async def process_message(
    message: aio_pika.abc.AbstractIncomingMessage,
    connection: aio_pika.Connection,  # Add connection parameter
) -> None:
    async with message.process():
        try:
            async with timeout(1.5):
                body: dict = json.loads(message.body)

                username: str = body['username']
                amount: int = body['amount']

                print(f" [x] Received {body}")

                # Create Order.
                async with SessionLocal() as db:
                    is_created = await crud.create_order(db=db, username=username, amount=amount)
                    print(f"create order")
                    if is_created is not None:
                        routing_key = "from.order"
                        body['order_number'] = is_created.id

                        channel = await connection.channel()

                        await channel.default_exchange.publish(
                            aio_pika.Message(body=bytes(json.dumps(body), 'utf-8')),
                            routing_key=routing_key,
                        )

                        await db.commit()
                    else:
                        # Nothing happens
                        print("ROLL BACK")
        except TimeoutError:
            print("Timed out")
        except Exception as e:
            print(f"Error: {e}")


async def process_rb(
    message: aio_pika.abc.AbstractIncomingMessage,
    connection: aio_pika.Connection,  # Add connection parameter
) -> None:
    async with message.process():
        body: dict = json.loads(message.body)

        order_id = body["order_number"]

        print(f" [x] Rolling Back {body}")

        async with SessionLocal() as db:
            is_rolled_back = await crud.change_status(db, order_id=order_id, status=body.get("status", "UNKNOWN"))

            if is_rolled_back:
                # Done
                await db.commit();
                print("Rolled Back Succesfully")
                pass
            else:
                print("GG[0]")


async def process_complete(
    message: aio_pika.abc.AbstractIncomingMessage,
    connection: aio_pika.Connection,  # Add connection parameter
) -> None:
    try:
        # If literally anything happens here, WHY???, well rolling back everything
        async with message.process():
            body: dict = json.loads(message.body)

            order_id = body["order_number"]

            async with SessionLocal() as db:
                is_done = await crud.change_status(db, order_id=order_id, status="SUCCESS")

                if is_done:
                    # Done
                    await db.commit();
                    print("SUCCESS")
                    pass
                else:
                    print("GG[0]")
    except:
        print(f" [x] Rolling Back {body}")

        channel = await connection.channel()

        await channel.default_exchange.publish(
            aio_pika.Message(body=message.body),
            routing_key="rb.deliver",
        )


async def main() -> None:
    connection = await aio_pika.connect_robust(
        "amqp://rabbit-mq",
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
    queue_rb = await channel.declare_queue("rb.order");
    queue_complete = await channel.declare_queue("to.order.complete")

    print(' [*] Waiting for messages. To exit press CTRL+C')

    await queue.consume(lambda message: process_message(message, connection))
    await queue_rb.consume(lambda message: process_rb(message, connection))
    await queue_complete.consume(lambda message: process_complete(message, connection))

    try:
        # Wait until terminate
        await asyncio.Future()
    finally:
        await connection.close()


if __name__ == "__main__":
    asyncio.run(main())