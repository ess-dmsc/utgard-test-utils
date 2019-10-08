import json


class CommandProducer:
    def __init__(self, kafka_producer, topic):
        self._producer = kafka_producer
        self._topic = topic

    def produce(self, cmd):
        """Serialise and send a message to the Kafka cluster.

        Keyword arguments:
        cmd -- a dict to be sent as JSON
        """
        cmd_json = bytes(json.dumps(cmd), "ascii")
        self._producer.produce(self._topic, cmd_json)


class StatusConsumer:
    def __init__(self, kafka_consumer, topic):
        self._consumer = kafka_consumer
        self._topic = topic

    def start(self):
        """Start subscribing to the Kakfa topic.

        This method must be called before get_messages is called for the first
        time.
        """
        self._consumer.subscribe([self._topic])

    def get_messages(self, num_messages, timeout_s):
        """Consume up to num_messages from the Kafka topic.

        Keyword arguments:
        num_messages -- maximum number of messages to consume
        timeout_s -- time in seconds to wait for messages

        Returns a (timestamp, payload) tuple.
        """
        messages = self._consumer.consume(num_messages, timeout_s)
        timestamps_and_messages = []
        for m in messages:
            if m.error() is None:
                ts_type, ts = m.timestamp()
                status = m.value()
                timestamps_and_messages.append([ts, status])

        return timestamps_and_messages

    def stop(self):
        """End subscription and close consumer."""
        self._consumer.unsubscribe()
        self._consumer.close()
