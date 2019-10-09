import logging
import threading
import time


class StatusThread(threading.Thread):
    SLEEP_TIME_S = 0.5

    def __init__(
        self, status_processor, logger=logging.getLogger(__name__)
    ):
        super().__init__()
        self._status_processor = status_processor
        self._logger = logger
        self._stop_event = threading.Event()

    def is_file_writer_running(self):
        return self._status_processor.is_writing()

    def get_metrics(self):
        return self._status_processor.get_metrics()

    def run(self):
        self._status_processor.start()
        try:
            while not self._stop_event.wait(0):
                self._status_processor.update_status()
                time.sleep(self.SLEEP_TIME_S)
        finally:
            self._status_processor.stop()

    def stop(self):
        self._stop_event.set()
