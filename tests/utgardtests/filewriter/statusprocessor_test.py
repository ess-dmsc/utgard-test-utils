import pytest
from utgardtests.filewriter import statusprocessor


def get_filewriter_status_master_msg(kafka_timestamp):
    payload = {
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

    return kafka_timestamp, payload


class TestIsFileWriterWriting:
    def fake_time_fun(self):
        return 11.0

    @pytest.fixture
    def sp(self):
        return statusprocessor.StatusProcessor(
            "filewriter-1", "unit-test-1", self.fake_time_fun
        )

    def test_status_processor_just_created(self, sp):
        assert not sp.is_file_writer_writing()

    def test_filewriter_is_writing(self, sp):
        sp.process_msg(get_filewriter_status_master_msg(9.0))
        assert sp.is_file_writer_writing()
        sp.process_msg(get_filewriter_status_master_msg(10.0))
        assert sp.is_file_writer_writing()

    def test_filewriter_stopped_writing(self, sp):
        sp.process_msg(get_filewriter_status_master_msg(0.0))
        assert not sp.is_file_writer_writing()


class TestMetrics:
    def get_stream_master_status_msg(self, timestamp, value):
        kafka_timestamp = 0
        payload = {
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

        return kafka_timestamp, payload

    @pytest.fixture
    def sp(self):
        return statusprocessor.StatusProcessor("filewriter-1", "unit-test-1")

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