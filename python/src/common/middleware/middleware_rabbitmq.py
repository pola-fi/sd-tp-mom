import pika
from pika.exceptions import AMQPConnectionError, AMQPError

from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareCloseError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
)


class _BlockingRabbitClient:

    def __init__(self, host):
        self.host = host
        self.connection = None
        self.channel = None
        self._consumer_active = False
        self.bound_consumer_queue_name = None

    def _reset_broken_session(self):
        self._safely_close_channel_and_connection()
        self.connection = None
        self.channel = None

    def _safely_close_channel_and_connection(self):
        if self.channel is not None and self.channel.is_open:
            self.channel.close()
        if self.connection is not None and self.connection.is_open:
            self.connection.close()

    def connect(self):
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(self.host)
            )
            self.channel = self.connection.channel()
        except Exception as exc:
            self._reset_broken_session()
            raise MessageMiddlewareDisconnectedError() from exc

    @staticmethod
    def _wrap_user_callback(on_message_callback):
        def pika_callback(ch, method, properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

            on_message_callback(body, ack, nack)

        return pika_callback

    def _consume_loop(self, on_message_callback, setup, resolve_queue_name, prefetch=None):
        pika_cb = self._wrap_user_callback(on_message_callback)
        try:
            setup()
            queue_name = resolve_queue_name()
            if prefetch is not None:
                self.channel.basic_qos(prefetch_count=prefetch)
            self.channel.basic_consume(
                queue=queue_name,
                auto_ack=False,
                on_message_callback=pika_cb,
            )
            self._consumer_active = True
            try:
                self.channel.start_consuming()
            finally:
                self._consumer_active = False
        except AMQPConnectionError as exc:
            raise MessageMiddlewareDisconnectedError() from exc
        except AMQPError as exc:
            raise MessageMiddlewareMessageError() from exc

    def consume_from_queue(self, queue_name, on_message_callback, prefetch=None):
        def setup():
            self.channel.queue_declare(queue=queue_name, durable=False)

        self._consume_loop(
            on_message_callback,
            setup,
            lambda: queue_name,
            prefetch,
        )

    def _declare_direct_exchange(self, exchange_name):
        self.channel.exchange_declare(
            exchange=exchange_name,
            exchange_type="direct",
            durable=False,
        )

    def consume_from_exchange(
        self, exchange_name, routing_keys, on_message_callback
    ):
        def setup():
            self._declare_direct_exchange(exchange_name)
            result = self.channel.queue_declare(queue="", exclusive=True)
            self.bound_consumer_queue_name = result.method.queue
            for rk in routing_keys:
                self.channel.queue_bind(
                    queue=self.bound_consumer_queue_name,
                    exchange=exchange_name,
                    routing_key=rk,
                )

        self._consume_loop(
            on_message_callback,
            setup,
            lambda: self.bound_consumer_queue_name,
            prefetch=1,
        )

    def publish_to_queue(self, queue_name, body):
        def op():
            self.channel.queue_declare(queue=queue_name, durable=False)
            self.channel.basic_publish(
                exchange="",
                routing_key=queue_name,
                body=body,
            )

        self._run_publish(op)

    def publish_to_exchange(self, exchange_name, routing_keys, body):
        def op():
            self._declare_direct_exchange(exchange_name)
            for rk in routing_keys:
                self.channel.basic_publish(
                    exchange=exchange_name,
                    routing_key=rk,
                    body=body,
                )

        self._run_publish(op)

    def _run_publish(self, operation):
        try:
            operation()
        except AMQPConnectionError as exc:
            raise MessageMiddlewareDisconnectedError() from exc
        except AMQPError as exc:
            raise MessageMiddlewareMessageError() from exc

    def stop_consuming(self):
        if self.channel is None or not self.channel.is_open:
            return
        if not self._consumer_active:
            return
        try:
            self.channel.stop_consuming()
        except Exception as exc:
            raise MessageMiddlewareDisconnectedError() from exc

    def close(self):
        try:
            self.stop_consuming()
            self._safely_close_channel_and_connection()
        except Exception as exc:
            raise MessageMiddlewareCloseError() from exc


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.queue_name = queue_name
        self._client = _BlockingRabbitClient(host)
        self._client.connect()

    def start_consuming(self, on_message_callback):
        self._client.consume_from_queue(self.queue_name, on_message_callback)

    def stop_consuming(self):
        self._client.stop_consuming()

    def send(self, message):
        self._client.publish_to_queue(self.queue_name, message)

    def close(self):
        self._client.close()


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):

    def __init__(self, host, exchange_name, routing_keys):
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self._client = _BlockingRabbitClient(host)
        self._client.connect()

    def start_consuming(self, on_message_callback):
        self._client.consume_from_exchange(
            self.exchange_name,
            self.routing_keys,
            on_message_callback,
        )

    def stop_consuming(self):
        self._client.stop_consuming()

    def send(self, message):
        self._client.publish_to_exchange(
            self.exchange_name,
            self.routing_keys,
            message,
        )

    def close(self):
        self._client.close()
