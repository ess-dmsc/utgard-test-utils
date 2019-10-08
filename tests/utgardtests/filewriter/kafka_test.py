import pytest
import confluent_kafka
from utgardtests.filewriter import kafka


class TestCommandProducer:
    def test_produce(self, mocker):
        kafka_producer = mocker.Mock()
        p = kafka.CommandProducer(kafka_producer, "test-topic")
        p.produce({'key': 'value'})
        assert kafka_producer.produce.called


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


class TestStatusConsumer:
    @pytest.fixture
    def sc(self):
        return kafka.StatusConsumer(KafkaConsumerStub(), "test-topic")

    def test_valid_messages(self, sc):
        sc.start()
        msgs = sc.get_messages(5, 1)
        sc.stop()
        assert len(msgs) == 5
        assert msgs[2] == [2, "2"]

    def test_messages_with_error(self, sc):
        sc.start()
        msgs = sc.get_messages(10, 1)
        sc.stop()
        assert len(msgs) == 9
        assert msgs[0] == [0, "0"]
        assert msgs[-1] == [9, "9"]
