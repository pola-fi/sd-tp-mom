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
            self.channel.queue_declare(queue=queue_name, durable=True)
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


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        pass

    def start_consuming(self, on_message_callback):
        pass

    def stop_consuming(self):
        pass

    def send(self, message):
        pass

    def close(self):
        pass
