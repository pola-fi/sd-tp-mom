import pika

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
    
    def _reset_broken_session(self):
        self._safely_close_channel_and_connection()
        self.connection = None
        self.channel = None

    def _safely_close_channel_and_connection(self):
        if self.channel is not None and self.channel.is_open:
            self.channel.close()
        if self.connection is not None and self.connection.is_open:
            self.connection.close()

    def _stop_consuming_safe(self):
        if self.channel is None or not self.channel.is_open:
            return
        if not self._consumer_active:
            return
        try:
            self.channel.stop_consuming()
        except Exception as exc:
            raise MessageMiddlewareDisconnectedError() from exc

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
    def wrap_user_callback(on_message_callback):
        def pika_callback(ch, method, properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

            on_message_callback(body, ack, nack)

        return pika_callback

    def consume_messages(
        self, on_message_callback, *, setup_before_consume, resolve_queue_name, prefetch=None
    ):
        pika_cb = self.wrap_user_callback(on_message_callback)
        try:
            setup_before_consume()
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
        except Exception as exc:
            raise MessageMiddlewareDisconnectedError() from exc

    def send(self, publish_fn):
        try:
            return publish_fn()
        except Exception as exc:
            raise MessageMiddlewareMessageError() from exc

    def close(self):
        try:
            self._stop_consuming_safe()
            self._safely_close_channel_and_connection()
        except Exception as exc:
            raise MessageMiddlewareCloseError() from exc


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.queue_name = queue_name
        self._client = _BlockingRabbitClient(host)
        self._client.connect()

    @property
    def connection(self):
        return self._client.connection

    @property
    def channel(self):
        return self._client.channel

    def _declare_listen_queue(self):
        self._client.channel.queue_declare(queue=self.queue_name, durable=False)

    def start_consuming(self, on_message_callback):
        self._client.consume_messages(
            on_message_callback,
            setup_before_consume=self._declare_listen_queue,
            resolve_queue_name=lambda: self.queue_name,
        )

    def stop_consuming(self):
        self._client._stop_consuming_safe()

    def send(self, message):
        def _publish():
            self._declare_listen_queue()
            self._client.channel.basic_publish(
                exchange="",
                routing_key=self.queue_name,
                body=message,
            )

        self._client.send(_publish)

    def close(self):
        self._client.close()


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):

    def __init__(self, host, exchange_name, routing_keys):
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.queue_name = None
        self._client = _BlockingRabbitClient(host)
        self._client.connect()

    @property
    def connection(self):
        return self._client.connection

    @property
    def channel(self):
        return self._client.channel

    def _declare_exchange(self):
        self._client.channel.exchange_declare(
            exchange=self.exchange_name,
            exchange_type="direct",
            durable=False,
        )

    def _declare_consume_topology(self):
        self._declare_exchange()
        result = self._client.channel.queue_declare(queue="", exclusive=True)
        self.queue_name = result.method.queue
        for rk in self.routing_keys:
            self._client.channel.queue_bind(
                queue=self.queue_name,
                exchange=self.exchange_name,
                routing_key=rk,
            )

    def start_consuming(self, on_message_callback):
        self._client.consume_messages(
            on_message_callback,
            setup_before_consume=self._declare_consume_topology,
            resolve_queue_name=lambda: self.queue_name,
            prefetch=1,
        )

    def stop_consuming(self):
        self._client._stop_consuming_safe()

    def send(self, message):
        def _publish_all():
            self._declare_exchange()
            for rk in self.routing_keys:
                self._client.channel.basic_publish(
                    exchange=self.exchange_name,
                    routing_key=rk,
                    body=message,
                )

        self._client.send(_publish_all)

    def close(self):
        self._client.close()
