import time
import pandas


class StatusProcessor:
    LIVENESS_TIMEOUT_S = 10.0

    def __init__(self, service_id, job_id, time_fun=time.time):
        self._service_id = service_id
        self._job_id = job_id
        self._time_fun = time_fun
        self._last_status_timestamp_s = None
        self._metrics = {}

    def is_file_writer_writing(self):
        """Get file writer writing status for instance's job_id.

        This method compares the timestamp from the last file writer status
        master message containing an entry with the instance's job_id to the
        current time.
        """
        if self._last_status_timestamp_s is None:
            return False
        else:
            delta_time_s = self._time_fun() - self._last_status_timestamp_s
            return delta_time_s <= self.LIVENESS_TIMEOUT_S

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
        except KeyError:
            print("Error")
            # Log error.

    def _process_filewriter_status_master_msg(self, msg):
        if msg[1]["service_id"] == self._service_id:
            self._process_filewriter_status_master_files(msg)

    def _process_filewriter_status_master_files(self, msg):
        if self._job_id in msg[1]["files"]:
            self._last_status_timestamp_s = msg[0]

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
