import json
import time
import numpy
import pandas
import requests


class ServerMetricsClient:
    def __init__(self, graphite_render_url):
        self.graphite_render_url = graphite_render_url

    def get_collectd_metrics(
        self, servers, metric_targets, start_time, end_time
    ):
        target_template = "system.{{{}}}.collectd.{}"
        return self._get_metrics(
            servers,
            metric_targets,
            start_time,
            end_time,
            target_template,
            lambda x: ".".join(x[3:]),
        )

    def get_kafka_metrics(self, servers, metric_targets, start_time, end_time):
        target_template = "kafka.{{{}}}.{}"
        return self._get_metrics(
            servers,
            metric_targets,
            start_time,
            end_time,
            target_template,
            lambda x: ".".join(x[2:]),
        )

    def _get_metrics(
        self,
        servers,
        metric_targets,
        start_time,
        end_time,
        target_template,
        metric_name_function,
    ):
        metrics_series = dict((s, {}) for s in servers)
        for metric_target in metric_targets:
            target = target_template.format(",".join(servers), metric_target)
            graphite = GraphiteClient(
                self.graphite_render_url, target, start_time, end_time
            )
            data = graphite.get_metrics()
            time.sleep(0.010)
            for d in data:
                target_components = d["target"].split(".")
                server = target_components[1]
                metric_name = metric_name_function(target_components)
                datapoints = numpy.array(d["datapoints"])
                s = pandas.Series(datapoints[:, 0], index=datapoints[:, 1])
                metrics_series[server][metric_name] = s

        metrics = {}
        for server in metrics_series:
            metrics[server] = pandas.DataFrame(metrics_series[server])

        return metrics


class GraphiteClient:
    def __init__(self, graphite_render_url, target, start_time, end_time=None):
        self._query = GraphiteQueryBuilder(
            graphite_render_url, target, start_time, end_time
        )

    def get_metrics(self):
        r = requests.get(self._query.build_query())
        if r.status_code == 200:
            return json.loads(r.text)
        else:
            raise Exception("Status code {} is not 200".format(r.status_code))


class GraphiteQueryBuilder:
    def __init__(self, graphite_render_url, target, start_time, end_time):
        self.base_url = graphite_render_url
        self.target = target
        self.start_time = start_time
        self.end_time = end_time

    def build_query(self):
        q = self.base_url
        q = q + "?target={}".format(self.target)
        q = q + "&from={}".format(self.start_time)
        if self.end_time is not None:
            q = q + "&until={}".format(self.end_time)
        q = q + "&format=json"
        return q
