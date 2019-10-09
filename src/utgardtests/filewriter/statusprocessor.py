import logging
import threading
import time


class StatusProcessor:
    MAX_NUM_MESSAGES_PER_UPDATE = 10
    GET_MESSAGES_TIMEOUT_S = 0.5
    LIVENESS_TIMEOUT_S = 5

    def __init__(
        self,
        status_consumer,
        msg_processor,
        logger=logging.getLogger(__name__),
        time_function=time.time,
    ):
        self._consumer = status_consumer
        self._msg_processor = msg_processor
        self._logger = logger
        self._time_function = time_function
        self._is_writing = False
        self._is_writing_lock = threading.Lock()

    def start(self):
        self._consumer.start()

    def update_status(self):
        self._get_and_process_messages()
        self._update_running_status()

    def _get_and_process_messages(self):
        msgs = self._consumer.get_messages(
            self.MAX_NUM_MESSAGES_PER_UPDATE, self.GET_MESSAGES_TIMEOUT_S
        )
        for msg in msgs:
            self._msg_processor.process_msg(msg)

    def _update_running_status(self):
        ts = self._msg_processor.get_latest_timestamp()
        if ts is None:
            return

        ct = self._time_function()
        with self._is_writing_lock:
            self._is_writing = (ct - ts) <= self.LIVENESS_TIMEOUT_S

    def is_writing(self):
        with self._is_writing_lock:
            status = self._is_writing
        return status

    def stop(self):
        self._consumer.stop()

    def get_metrics(self):
        if self.is_writing():
            raise RuntimeError("File writer is still running")
        else:
            return self._msg_processor.get_metrics()
