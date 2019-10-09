class FileWriterClient:
    def __init__(self, service_id, job_id, kafka_producer, status_thread):
        self._service_id = service_id
        self._job_id = job_id
        self._producer = kafka_producer
        self._status_thread = status_thread

    def start(self, data_broker, nexus_structure):
        cmd = {
            "cmd": "FileWriter_new",
            "job_id": self._job_id,
            "broker": data_broker,
            "start_time": 0,
            "end_time": 1,
            "service_id": self._service_id,
            "file_attributes": {"file_name": "/tmp/file.nxs"},
            "nexus_structure": nexus_structure,
        }
        self._producer.produce(cmd)
        self._status_thread.start()
        # Block waiting for status monitoring thread running confirmation

    def stop(self, join_timeout_s=5):
        cmd = {
            "cmd": "FileWriter_stop",
            "job_id": self._job_id,
            "service_id": self._service_id,
        }
        self._producer.produce(cmd)
        self._status_thread.stop()
        self._status_thread.join(join_timeout_s)

    def get_metrics(self):
        return self._status_thread.get_metrics()

    def is_writing(self):
        return self._status_thread.is_file_writer_writing()
