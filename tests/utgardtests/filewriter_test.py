import pytest
from utgardtests import filewriter


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


class TestStatusProcessor:
    filewriter_status_master_msg = {
        "files": {
            "unit-test-1": {
                "filename": "/var/opt/dm_group/kafka-to-nexus/data.nxs",
                "topics": {
                    "FAKE_detector": {
                        "error_message_too_small": 0,
                        "error_no_flatbuffer_reader": 1,
                        "error_no_source_instance": 2,
                        "messages_processed": 3,
                    },
                    "FAKE_counters": {
                        "error_message_too_small": 4,
                        "error_no_flatbuffer_reader": 5,
                        "error_no_source_instance": 6,
                        "messages_processed": 7,
                    },
                },
            }
        },
        "service_id": "filewriter-1",
        "type": "filewriter_status_master",
    }

    def get_stream_master_status_msg(self, timestamp, value):
        return {
            "job_id": "unit-test-1",
            "next_message_eta_ms": 2000,
            "stream_master": {
                "Mbytes": 3000,
                "errors": 1,
                "messages": 200,
                "runtime": 1000,
                "state": "Running",
            },
            "streamer": {
                "FAKE_detector": {
                    "rates": {
                        "Mbytes": value,
                        "errors": 2,
                        "message_size": {
                            "average": 15.0,
                            "standard_deviation": 3.0,
                        },
                        "messages": 30,
                    }
                },
                "FAKE_counters": {
                    "rates": {
                        "Mbytes": 30,
                        "errors": 4,
                        "message_size": {
                            "average": 50.0,
                            "standard_deviation": 0.0,
                        },
                        "messages": 10,
                    }
                },
            },
            "timestamp": timestamp,
            "type": "stream_master_status",
        }

    @pytest.fixture
    def sp(self):
        return filewriter.StatusProcessor("filewriter-1", "unit-test-1")

    def test_process_filewriter_status_master_msg(self, sp):
        sp.process_msg(self.filewriter_status_master_msg)

    def test_process_stream_master_status_msg(self, sp):
        sp.process_msg(self.get_stream_master_status_msg(1, 100.0))
        sp.process_msg(self.get_stream_master_status_msg(2, 234.5))
        sp.process_msg(self.get_stream_master_status_msg(3, 999.9))
        metrics = sp.get_metrics()

        assert "FAKE_detector" in metrics
        assert len(metrics["FAKE_detector"]) == 3
        assert metrics["FAKE_detector"].index[0] == 1
        assert metrics["FAKE_detector"].iloc[0] == 100.0
        assert metrics["FAKE_detector"].iloc[1] == 234.5
        assert metrics["FAKE_detector"].iloc[2] == 999.9
        assert "FAKE_counters" in metrics
        assert len(metrics["FAKE_counters"]) == 3
        assert metrics["FAKE_counters"].index[0] == 1
        assert metrics["FAKE_counters"].iloc[2] == 30
