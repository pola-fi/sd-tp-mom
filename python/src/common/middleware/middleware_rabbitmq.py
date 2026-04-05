import pika
import random
import string
from .middleware import (
    MessageMiddlewareQueue, 
    MessageMiddlewareExchange,
    MessageMiddlewareCloseError,
    MessageMiddlewareMessageError
    )

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name
        self.connection = None
        self.channel = None
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=queue_name, durable=False)
        except Exception as exc:
            raise MessageMiddlewareMessageError() from exc

    def start_consuming(self, on_message_callback):

        def pika_callback(ch, method, properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)
            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            on_message_callback(body, ack, nack)


        self.channel.basic_consume(
            queue=self.queue_name,
            auto_ack=False,
            on_message_callback=pika_callback,
        )

        self.channel.start_consuming()

    def stop_consuming(self):
        self.channel.stop_consuming()

    def send(self, message):
        self.channel.basic_publish(exchange='',
                      routing_key=self.queue_name,
                      body=message)

    def close(self):
        try:
            self.connection.close()
        except Exception as exc:
            raise MessageMiddlewareCloseError() from exc


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self.host = host
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.connection = None
        self.channel = None
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
            self.channel = self.connection.channel()
            self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='fanout')
        except Exception as exc:
            raise MessageMiddlewareMessageError() from exc

    def start_consuming(self, on_message_callback):
        pass

    def stop_consuming(self):
        pass

    def send(self, message):
        pass

    def close(self):
        try:
            self.connection.close()
        except Exception as exc:
            raise MessageMiddlewareCloseError() from exc
