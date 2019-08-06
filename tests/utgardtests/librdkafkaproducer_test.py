import queue
import fabric
import pytest
import utgardtests.librdkafkaproducer as prod


class TestProducerSimpleCommand:
    def fabric_connection_run_stub(self, *args, **kwargs):
        return 0

    @pytest.fixture
    def producer(self, monkeypatch):
        monkeypatch.setattr(
            fabric.Connection, "run", self.fabric_connection_run_stub
        )

        log = queue.Queue()
        producer = prod.LibrdkafkaProducer(
            name="producer",
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
        producer.join(10)

        assert producer.start_time is not None
        assert producer.end_time is not None
        assert producer.result == 0

        n = 0
        while not producer.log.empty():
            producer.log.get()
            n = n + 1

        assert n == 2


class TestProducerCompleteCommand:
    def test_command(self):
        log = queue.Queue()
        producer = prod.LibrdkafkaProducer(
            name="producer",
            server="localhost",
            log=log,
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            msg_size=1000,
            msg_count=2000,
            output_path="/tmp",
            configs=["message.max.bytes=16777216"],
            rate=100,
            partition=0,
            acks=-1,
        )

        assert producer.cmd.find("-X message.max.bytes=16777216") > 0
        assert producer.cmd.find("-r 100") > 0
        assert producer.cmd.find("-p 0") > 0
        assert producer.cmd.find("-a -1") > 0


class TestProducerMetricsAndErrors:
    def fabric_connection_get_stub(self, *args, **kwargs):
        pass

    def read_producer_output_stub(self, *args, **kwargs):
        return prod._parse_producer_output(producer_output)

    def read_producer_errors_stub(self, *args, **kwargs):
        return ["Error 1", "Error 2"]

    def test_metrics(self, monkeypatch):
        monkeypatch.setattr(
            fabric.Connection, "get", self.fabric_connection_get_stub
        )

        monkeypatch.setattr(
            prod, "_read_producer_output", self.read_producer_output_stub
        )

        monkeypatch.setattr(
            prod, "_read_producer_errors", self.read_producer_errors_stub
        )

        log = queue.Queue()
        producer = prod.LibrdkafkaProducer(
            name="producer1",
            server="localhost",
            log=log,
            bootstrap_servers="localhost:9092",
            topic="test-topic",
            msg_size=1000,
            msg_count=2000,
            output_path="/tmp"
        )

        metrics, errors = prod.get_producer_metrics_and_errors([producer])

        assert "producer1" in metrics
        assert metrics['producer1'].shape == (7, 11)

        assert "producer1" in errors
        assert len(errors['producer1']) == 2


producer_output = [
    "% Sending 2000 messages of size 1000000 bytes",
    "|    elapsed |       msgs |      bytes |        rtt |         dr |     dr_m/s |    dr_MB/s |     dr_err |     tx_err |       outq |     offset",
    "|       1000 |       1073 | 1073000000 |         92 |          0 |          0 |       0.00 |          0 |         99 |       1073 |          0",
    "|       2000 |       1425 | 1425000000 |         14 |        352 |        175 |     175.94 |          0 |        208 |       1073 |        351",
    "|       3001 |       1825 | 1825000000 |         27 |        752 |        250 |     250.57 |          0 |        318 |       1073 |        751",
    "|       4001 |       2000 | 2000000000 |         29 |       1152 |        287 |     287.89 |          0 |        359 |        848 |       1151",
    "|       5002 |       2000 | 2000000000 |         29 |       1568 |        313 |     313.47 |          0 |        359 |        432 |       1567",
    "|       6002 |       2000 | 2000000000 |         29 |       1984 |        330 |     330.53 |          0 |        359 |         16 |       1983",
    "% 359 backpressures for 2000 produce calls: 17.950% backpressure rate",
    "|       6028 |       2000 | 2000000000 |         29 |       2000 |        331 |     331.73 |          0 |        359 |          0 |       1999",
]
