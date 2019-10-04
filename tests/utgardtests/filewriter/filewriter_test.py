import pytest
from utgardtests.filewriter import filewriter


class KafkaProducerStub:
    def __init__(self, broker, topic):
        pass

    def produce(self, msg):
        pass


class TestFileWriterClient:
    @pytest.fixture
    def fw(self):
        kafka_producer = KafkaProducerStub(None, None)
        return filewriter.FileWriterClient(None, None, kafka_producer)

    def test_not_running_after_construction(self, fw):
        assert not fw.is_running()

    def test_is_running_after_start(self, fw):
        fw.start(None, None)
        assert fw.is_running()

    def test_not_running_after_stop(self, fw):
        fw.start(None, None)
        fw.stop()
        assert not fw.is_running()
