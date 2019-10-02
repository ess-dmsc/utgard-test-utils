import json
import time
import confluent_kafka
import pandas


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


class KafkaProducer:
    def __init__(self, broker, topic):
        self._config = {"bootstrap.servers": self._cmd_broker}
        self._topic = topic

    def produce(self, msg):
        """Serialise and send a message to the Kafka cluster.

        Keyword arguments:
        msg -- a dict to be sent as JSON
        """
        producer = confluent_kafka.Producer(self._config)
        msg_json = bytes(json.dumps(msg), "ascii")
        producer(self._topic, msg_json)


class StatusProcessor:
    def __init__(self, service_id, job_id):
        self._service_id = service_id
        self._job_id = job_id
        self._last_status_time = None
        self._metrics = {}
        self._liveness_timeout_s = 10.0

    def process_msg(self, msg):
        try:
            msg_type = msg["type"]
            if msg_type == "filewriter_status_master":
                self._process_filewriter_status_master_msg(msg)
            elif msg_type == "stream_master_status":
                self._process_stream_master_status_msg(msg)
            else:
                return
        except KeyError:
            print("Error")
            return

    def is_file_writer_alive(self):
        if self._last_status_time is None:
            return False
        else:
            delta_time_s = time.monotonic() - self._last_status_time_s
            return delta_time_s <= self._liveness_timeout_s

    def get_metrics(self):
        r = dict()
        for key in self._metrics:
            values = self._metrics[key]['values']
            timestamps = self._metrics[key]['timestamps']
            r[key] = pandas.Series(values, timestamps)

        return r

    def _process_filewriter_status_master_msg(self, msg):
        if msg["service_id"] == self._service_id:
            self._process_filewriter_status_master_files(msg)
        else:
            return

    def _process_filewriter_status_master_files(self, msg):
        if self._job_id in msg["files"]:
            self._last_status_time = time.monotonic()
        else:
            return

    def _process_stream_master_status_msg(self, msg):
        if msg["job_id"] == self._job_id:
            self._extract_stream_master_status_metrics(msg)
        else:
            return

    def _extract_stream_master_status_metrics(self, msg):
        timestamp = msg["timestamp"]
        streamers = msg["streamer"]
        for streamer in streamers:
            if streamer not in self._metrics:
                self._metrics[streamer] = {
                    'timestamps': [],
                    'values': []
                }
            data = streamers[streamer]["rates"]["Mbytes"]
            self._metrics[streamer]['values'].append(data)
            self._metrics[streamer]['timestamps'].append(timestamp)
