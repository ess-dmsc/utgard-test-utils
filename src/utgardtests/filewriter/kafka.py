import json
import confluent_kafka


class KafkaProducer:
    def __init__(self, broker, topic):
        self._config = {"bootstrap.servers": broker}
        self._topic = topic

    def produce(self, cmd):
        """Serialise and send a message to the Kafka cluster.

        Keyword arguments:
        cmd -- a dict to be sent as JSON
        """
        producer = confluent_kafka.Producer(self._config)
        cmd_json = bytes(json.dumps(cmd), "ascii")
        producer(self._topic, cmd_json)


class KafkaConsumer:
    MAX_MESSAGES = 10

    def __init__(self, broker, topic, group_id):
        config = {"bootstrap.servers": broker, "group.id": group_id}
        self._consumer = confluent_kafka.Consumer(config)
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
        messages = self._consumer.consume(self.MAX_MESSAGES, timeout_s)
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
