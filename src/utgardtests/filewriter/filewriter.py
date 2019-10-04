class FileWriterClient:
    def __init__(self, service_id, job_id, kafka_producer):
        self._service_id = service_id
        self._job_id = job_id
        self._producer = kafka_producer
        self._running = False

    def start(self, data_broker, nexus_structure):
        self._running = True
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
        # Start status monitoring thread
        # Block waiting for status monitoring thread running confirmation

    def stop(self):
        cmd = {
            "cmd": "FileWriter_stop",
            "job_id": self._job_id,
            "service_id": self._service_id,
        }
        self._producer.produce(cmd)
        self._running = False

    def get_metrics(self):
        pass

    def is_running(self):
        return self._running
