import confluent_kafka
from utgardtests.filewriter import kafka


class KafkaMessageStub:
    def __init__(self, timestamp, value):
        self._timestamp = timestamp
        self._value = value

    def timestamp(self):
        return confluent_kafka.TIMESTAMP_CREATE_TIME, self._timestamp

    def value(self):
        return self._value

    def error(self):
        if self._timestamp == 6:
            return True
        else:
            return None


class KafkaConsumerStub:
    def subscribe(self, topics):
        pass

    def consume(self, num_messages, timeout_s):
        return [KafkaMessageStub(int(i), str(i)) for i in range(num_messages)]

    def unsubscribe(self):
        pass

    def close(self):
        pass


class TestKafkaConsumer:
    def test_valid_messages(self):
        c = kafka.KafkaConsumer(KafkaConsumerStub(), "test-topic")
        c.start()
        msgs = c.get_messages(5, 1)
        c.stop()
        assert len(msgs) == 5
        assert msgs[2] == [2, "2"]

    def test_messages_with_error(self):
        c = kafka.KafkaConsumer(KafkaConsumerStub(), "test-topic")
        c.start()
        msgs = c.get_messages(10, 1)
        c.stop()
        assert len(msgs) == 9
        assert msgs[-1] == [9, "9"]
