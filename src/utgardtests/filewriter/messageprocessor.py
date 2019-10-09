import logging
import pandas


class MessageProcessor:
    def __init__(self, service_id, job_id, logger=logging.getLogger(__name__)):
        self._service_id = service_id
        self._job_id = job_id
        self._latest_timestamp = None
        self._metrics = {}
        self._logger = logger

    def get_latest_timestamp(self):
        return self._latest_timestamp

    def get_metrics(self):
        """Return the file writer metrics as a pandas.DataFrame."""
        r = dict()
        for key in self._metrics:
            values = self._metrics[key]["values"]
            timestamps = self._metrics[key]["timestamps"]
            r[key] = pandas.Series(values, timestamps)

        return r

    def process_msg(self, msg):
        """Process a message to extract file writer status and metrics.

        Keyword arguments:
        msg -- a (timestamp, payload) pair
        """
        try:
            msg_type = msg[1]["type"]
            if msg_type == "filewriter_status_master":
                self._process_filewriter_status_master_msg(msg)
            elif msg_type == "stream_master_status":
                self._process_stream_master_status_msg(msg)
        except KeyError as e:
            self._logger.warning("Key '{}' not found in message".format(e))

    def _process_filewriter_status_master_msg(self, msg):
        if msg[1]["service_id"] == self._service_id:
            self._process_filewriter_status_master_files(msg)

    def _process_filewriter_status_master_files(self, msg):
        if self._job_id in msg[1]["files"]:
            self._latest_timestamp = msg[0]

    def _process_stream_master_status_msg(self, msg):
        if msg[1]["job_id"] == self._job_id:
            self._extract_stream_master_status_metrics(msg)

    def _extract_stream_master_status_metrics(self, msg):
        timestamp = msg[1]["timestamp"]  # The file writer timestamp
        streamers = msg[1]["streamer"]
        for streamer in streamers:
            if streamer not in self._metrics:
                self._metrics[streamer] = {"timestamps": [], "values": []}
            data = streamers[streamer]["rates"]["Mbytes"]
            self._metrics[streamer]["values"].append(data)
            self._metrics[streamer]["timestamps"].append(timestamp)
