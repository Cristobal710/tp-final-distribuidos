"""Opt-in regression against a real broker: python3 -m tests.rabbitmq_publisher_check."""
import os
import time
import uuid

import pika

from common.middleware import MessageMiddlewareExchangeRabbitMQ, MessageMiddlewareQueueRabbitMQ
from common.middleware.middleware import MessageMiddlewareMessageError
from common.publisher import ReconnectingPublisher


def main():
    host = os.environ.get("RABBITMQ_TEST_HOST", "rabbitmq")
    prefix = "publisher-regression-" + uuid.uuid4().hex
    exchange, queue = prefix + "-control", prefix + "-data"
    routes = [prefix + "-0", prefix + "-1"]
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
    channel = connection.channel()
    channel.exchange_declare(exchange=exchange, exchange_type="direct", durable=True)
    for name in [queue, *routes]:
        channel.queue_declare(queue=name, durable=True)
    for route in routes:
        channel.queue_bind(queue=route, exchange=exchange, routing_key=route)
    connection.close()
    opened = []

    def open_queue():
        opened.append("data")
        return MessageMiddlewareQueueRabbitMQ(host, queue, heartbeat=4)

    def open_control():
        opened.append("control")
        return MessageMiddlewareExchangeRabbitMQ(host, exchange, routes, heartbeat=4)

    data = ReconnectingPublisher(open_queue, idle_seconds=2)
    control = ReconnectingPublisher(open_control, idle_seconds=2)
    eof_only = ReconnectingPublisher(open_control, idle_seconds=2)
    try:
        data.send(b"article")
        control.send(b"first")
        print("Waiting beyond the negotiated heartbeat timeout...", flush=True)
        time.sleep(12)
        data.send(b"EOF")
        control.send(b"EOF")
        eof_only.send(b"first publication after idle")
        assert opened.count("data") == 2 and opened.count("control") == 3, opened
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        channel = connection.channel()
        for name in [queue, *routes]:
            actual = []
            while True:
                method, _, body = channel.basic_get(queue=name, auto_ack=True)
                if method is None:
                    break
                actual.append(body)
            expected = [b"article", b"EOF"] if name == queue else [b"first", b"EOF", b"first publication after idle"]
            assert actual == expected, (name, actual)
        # A partial broadcast must fail without replaying its already confirmed first route.
        partial = ReconnectingPublisher(lambda: MessageMiddlewareExchangeRabbitMQ(
            host, exchange, [routes[0], prefix + "-missing"], heartbeat=4), idle_seconds=2)
        try:
            try:
                partial.send(b"partial")
            except MessageMiddlewareMessageError:
                pass
            else:
                raise AssertionError("Unroutable publication was accepted")
            assert channel.basic_get(queue=routes[0], auto_ack=True)[2] == b"partial"
            assert channel.basic_get(queue=routes[0], auto_ack=True)[0] is None, "Partial broadcast was replayed"
        finally:
            partial.close()
        print("PASS: idle queue/control publishers, lazy EOF, broker confirms, and no partial-broadcast replay", flush=True)
    finally:
        for publisher in (data, control, eof_only):
            publisher.close()
        if connection.is_closed:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            channel = connection.channel()
        for name in [queue, *routes]:
            channel.queue_delete(queue=name)
        channel.exchange_delete(exchange=exchange)
        connection.close()


if __name__ == "__main__":
    main()
