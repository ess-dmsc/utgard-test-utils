import queue
import fabric
import pytest
from utgardtests.librdkafkaproducer import LibrdkafkaProducer


class TestProducerSimpleCommand:
    def fabric_connection_run_stub(self, *args, **kwargs):
        return 0

    @pytest.fixture
    def producer(self, monkeypatch):
        monkeypatch.setattr(
            fabric.Connection, "run", self.fabric_connection_run_stub
        )

        log = queue.Queue()
        producer = LibrdkafkaProducer(
            server="localhost",
            log=log,
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            msg_size=1000,
            msg_count=2000,
            output_path="/tmp",
        )
        return producer

    def test_uninitialised_fields(self, producer):
        assert producer.result is None
        assert producer.start_time is None
        assert producer.end_time is None

    def test_command(self, producer):
        assert len(producer.cmd) > 0
        assert producer.cmd.find("-P ") > 0
        assert producer.cmd.find("-b localhost:9092") > 0
        assert producer.cmd.find("-t test-topic") > 0
        assert producer.cmd.find("-s 1000") > 0
        assert producer.cmd.find("-c 2000") > 0
        assert producer.cmd.find("-u") > 0

    def test_run(self, producer):
        producer.start()
        producer.join(1)

        assert producer.start_time > 0
        assert producer.end_time > 0
        assert producer.result == 0

        n = 0
        while not producer.log.empty():
            producer.log.get()
            n = n + 1

        assert n == 2


class TestProducerCompleteCommand:
    def test_command(self):
        log = queue.Queue()
        producer = LibrdkafkaProducer(
            server="localhost",
            log=log,
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            msg_size=1000,
            msg_count=2000,
            output_path="/tmp",
            configs=['message.max.bytes=16777216'],
            rate=100,
            partition=0,
            acks=-1
        )

        assert producer.cmd.find("-X message.max.bytes=16777216") > 0
        assert producer.cmd.find("-r 100") > 0
        assert producer.cmd.find("-p 0") > 0
        assert producer.cmd.find("-a -1") > 0
