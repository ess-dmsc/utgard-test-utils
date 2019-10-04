import threading


class StatusThread(threading.Thread):
    def __init__(self, kafka_consumer):
        super().__init__()
        self._consumer = kafka_consumer
        self._is_running = None
        self._is_running_lock = threading.Lock()

    def is_file_writer_running(self):
        with self._is_running_lock:
            status = self._is_running

        return status

    def run(self):
        pass
