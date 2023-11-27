from typing import List
import pika, sys, os, json
from dotenv import load_dotenv

import os
from requests import Session

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from db import crud
from db.engine import SessionLocal, engine



load_dotenv()
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def callback(ch, method, properties, body):
    body: dict = json.loads(body)

    username: str = body['username']
    user_credit: int = body['user_credit']
    inventory_uuid: int = body['inventory_uuid']
    pirce: int = body['pirce']
    amout: int = body['amout']

    print(f" [x] Received {body}")

    # Create Order.
    db: Session = SessionLocal()
    crud.create_order(db=db, username=username, inventory_uuid=inventory_uuid, total_purchase=amout * pirce)
    print(f"create order")

    ch.queue_declare(queue='from.order')

    ch.basic_publish(exchange='',
                        routing_key='from.order',
                        body=json.dumps(body))

    print(f" [x] Sent {json.dumps(body)}")

    ch.basic_ack(delivery_tag=method.delivery_tag)

    return


def main():
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbit-mq', port=5672))
    except:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))

    channel = connection.channel()

    channel.queue_declare(queue='to.order', arguments={
                          'x-message-ttl' : 1000,
                          'x-dead-letter-exchange' : 'dlx',
                          'x-dead-letter-routing-key' : 'dl'
                          })

    channel.basic_consume(queue='to.order', on_message_callback=callback)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)