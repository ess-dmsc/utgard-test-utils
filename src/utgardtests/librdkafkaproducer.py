import os
import threading
import time
import fabric


class LibrdkafkaProducer(threading.Thread):
    def __init__(
        self,
        server,
        log,
        bootstrap_servers,
        topic,
        msg_size,
        msg_count,
        output_path,
        **kwargs
    ):
        super().__init__()
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
