"""Thread for running an rdkafka_performance producer from librdkafka 1.1.0."""

import os
import tempfile
import threading
import time
import fabric
import numpy
import pandas


class LibrdkafkaProducer(threading.Thread):
    def __init__(
        self,
        name,
        server,
        log,
        bootstrap_servers,
        topic,
        msg_size,
        msg_count,
        output_path,
        **kwargs
    ):
        super().__init__(name=name)
        self.server = server
        self.log = log
        self.kwargs = kwargs
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.msg_size = msg_size
        self.msg_count = msg_count
        self.output_path = output_path
        self.kwargs = kwargs

        self.result = None
        self.start_time = None
        self.end_time = None

        self.cmd = self.build_cmd()

    def build_cmd(self):
        cmd = "rm -f {} {}".format(
            os.path.join(self.output_path, "output"),
            os.path.join(self.output_path, "error"),
        )
        cmd = cmd + " && mkdir -p {}".format(self.output_path)
        cmd = (
            cmd
            + " && /opt/dm_group/librdkafka/librdkafka-1.1.0/examples/rdkafka_performance -P -b {} -t {} -s {} -c {} -u".format(
                self.bootstrap_servers,
                self.topic,
                self.msg_size,
                self.msg_count,
            )
        )
        if "configs" in self.kwargs:
            for config in self.kwargs["configs"]:
                cmd = cmd + " -X {}".format(config)
        if "rate" in self.kwargs:
            cmd = cmd + " -r {}".format(self.kwargs["rate"])
        if "partition" in self.kwargs:
            cmd = cmd + " -p {}".format(self.kwargs["partition"])
        if "acks" in self.kwargs:
            cmd = cmd + " -a {}".format(self.kwargs["acks"])
        stdout_path = os.path.join(self.output_path, "output")
        stderr_path = os.path.join(self.output_path, "error")
        cmd = cmd + " 1>{} 2>{}".format(stdout_path, stderr_path)
        return cmd

    def run(self):
        self.start_time = round(time.time())
        self._add_to_log(
            "{} running command: '{}'".format(self.name, self.cmd)
        )
        with fabric.Connection(self.server) as c:
            self.result = c.run(self.cmd, hide=True, pty=True)
        self._add_to_log("{} finished running command".format(self.name))
        self.end_time = round(time.time())

    def _add_to_log(self, message):
        t = round(time.time())
        self.log.put("{}: {}".format(t, message))


def get_producer_metrics_and_errors(producers):
    """Get the results for a list of producers from remote servers."""
    producer_metrics = {}
    producer_errors = {}
    for producer in producers:
        with tempfile.TemporaryDirectory() as tmpdir:
            _copy_producer_files(producer.server, producer.output_path, tmpdir)
            producer_metrics[producer.name] = _read_producer_output(
                os.path.join(tmpdir, "output")
            )
            producer_errors[producer.name] = _read_producer_errors(
                os.path.join(tmpdir, "error")
            )

    return producer_metrics, producer_errors


def _copy_producer_files(server, remote_src, local_dst):
    with fabric.Connection(server) as c:
        c.get(
            os.path.join(remote_src, "output"),
            os.path.join(local_dst, "output"),
        )
        c.get(
            os.path.join(remote_src, "error"), os.path.join(local_dst, "error")
        )


def _read_producer_output(path):
    # _parse_producer_output would work with a file path, but we pass the file
    # to simplify testing.
    with open(path, "r") as f:
        data_frame = _parse_producer_output(f)

    return data_frame


def _parse_producer_output(output):
    data = numpy.genfromtxt(
        output,
        delimiter="|",
        comments="%",
        skip_header=2,
        usecols=range(1, 12),
        dtype=numpy.dtype(
            [
                ("time_ms", "u8"),
                ("msgs", "u8"),
                ("bytes", "u8"),
                ("rtt", "u8"),
                ("dr", "u8"),
                ("dr_msgs_per_s", "u8"),
                ("dr_MB_per_s", "f8"),
                ("dr_err", "u8"),
                ("tx_err", "u8"),
                ("queue", "u8"),
                ("offset", "u8"),
            ]
        ),
    )
    return pandas.DataFrame(data)


def _read_producer_errors(path):
    with open(path, "r") as f:
        errors = f.readlines()

    return errors
