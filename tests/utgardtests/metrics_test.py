import pytest
import requests
import utgardtests.metrics as metrics


class CollectdMetricsResponseStub:
    status_code = 200
    text = (
        "["
        '  {"target": "system.server1.collectd.metric.value3", "tags": {"name": "system.server1.collectd.metric.value3"}, "datapoints": [[0.05, 50], [0.05, 60], [0.05, 70], [0.05, 80]]},'  # noqa: E501
        '  {"target": "system.server1.collectd.metric.value2", "tags": {"name": "system.server1.collectd.metric.value2"}, "datapoints": [[0.01, 50], [0.01, 60], [0.01, 70], [0.01, 80]]},'  # noqa: E501
        '  {"target": "system.server1.collectd.metric.value1", "tags": {"name": "system.server1.collectd.metric.value1"}, "datapoints": [[0.0, 50], [0.0, 60], [0.0, 70], [0.0, 80]]},'      # noqa: E501
        '  {"target": "system.server2.collectd.metric.value3", "tags": {"name": "system.server2.collectd.metric.value3"}, "datapoints": [[0.05, 50], [0.05, 60], [0.05, 70], [0.05, 80]]},'  # noqa: E501
        '  {"target": "system.server2.collectd.metric.value2", "tags": {"name": "system.server2.collectd.metric.value2"}, "datapoints": [[0.01, 50], [0.01, 60], [0.01, 70], [0.01, 80]]},'  # noqa: E501
        '  {"target": "system.server2.collectd.metric.value1", "tags": {"name": "system.server2.collectd.metric.value1"}, "datapoints": [[0.0, 50], [0.0, 60], [0.0, 70], [0.0, 80]]}'       # noqa: E501
        "]"
    )


class KafkaMetricsResponseStub:
    status_code = 200
    text = (
        "["
        '  {"target": "kafka.server3.kafka.metric.value6", "tags": {"name": "kafka.server3.kafka.metric.value6"}, "datapoints": [[1, 100], [7, 110], [13, 120], [19, 130]]},'   # noqa: E501
        '  {"target": "kafka.server3.kafka.metric.value5", "tags": {"name": "kafka.server3.kafka.metric.value5"}, "datapoints": [[2, 100], [8, 110], [14, 120], [20, 130]]},'   # noqa: E501
        '  {"target": "kafka.server3.kafka.metric.value4", "tags": {"name": "kafka.server3.kafka.metric.value4"}, "datapoints": [[3, 100], [9, 110], [15, 120], [21, 130]]},'   # noqa: E501
        '  {"target": "kafka.server4.kafka.metric.value6", "tags": {"name": "kafka.server4.kafka.metric.value6"}, "datapoints": [[4, 100], [10, 110], [16, 120], [22, 130]]},'  # noqa: E501
        '  {"target": "kafka.server4.kafka.metric.value5", "tags": {"name": "kafka.server4.kafka.metric.value5"}, "datapoints": [[5, 100], [11, 110], [17, 120], [23, 130]]},'  # noqa: E501
        '  {"target": "kafka.server4.kafka.metric.value4", "tags": {"name": "kafka.server4.kafka.metric.value4"}, "datapoints": [[6, 100], [12, 110], [18, 120], [24, 130]]}'   # noqa: E501
        "]"
    )


class TestServerMetricsClient:
    def requests_get_collectd_stub(self, query):
        return CollectdMetricsResponseStub()

    def requests_get_kafka_stub(self, query):
        return KafkaMetricsResponseStub()

    def test_get_collectd_metrics(self, monkeypatch):
        monkeypatch.setattr(requests, "get", self.requests_get_collectd_stub)

        client = metrics.ServerMetricsClient("localhost")
        m = client.get_collectd_metrics(
            ["server1", "server2"], "metric.*", 50, 80
        )

        assert "server1" in m
        assert "server2" in m
        assert "metric.value1" in m["server1"]
        assert m["server1"].shape == (4, 3)
        assert m["server2"].loc[60, "metric.value2"] == 0.01

    def test_get_kafka_metrics(self, monkeypatch):
        monkeypatch.setattr(requests, "get", self.requests_get_kafka_stub)

        client = metrics.ServerMetricsClient("localhost")
        m = client.get_kafka_metrics(
            ["server3", "server4"], "metric.*", 100, 130
        )

        assert "server3" in m
        assert "server4" in m
        assert "kafka.metric.value5" in m["server4"]
        assert m["server4"].shape == (4, 3)
        assert m["server3"].loc[100, "kafka.metric.value4"] == 3


class GoodResponseStub:
    status_code = 200
    text = (
        '[{"target": "metric.value", "tags": {"name": "metric.value"}, '
        '"datapoints": [[0.1, 10], [0.5, 20], [1.2, 30]]}]'
    )


class BadResponseStub:
    status_code = 404


class TestGraphiteClient:
    def requests_get_good_stub(self, query):
        return GoodResponseStub()

    def requests_get_bad_stub(self, query):
        return BadResponseStub()

    def test_good_request(self, monkeypatch):
        monkeypatch.setattr(requests, "get", self.requests_get_good_stub)

        client = metrics.GraphiteClient("localhost", "metric.value", 1234)
        m = client.get_metrics()

        assert len(m) == 1
        assert m[0]["target"] == "metric.value"
        assert len(m[0]["datapoints"]) == 3
        assert [0.5, 20] in m[0]["datapoints"]

    def test_bad_request(self, monkeypatch):
        monkeypatch.setattr(requests, "get", self.requests_get_bad_stub)
        client = metrics.GraphiteClient("localhost", "metric.value", 1234)

        with pytest.raises(Exception):
            client.get_metrics()


class TestGraphiteQuery:
    def test_build_query(self):
        qb = metrics.GraphiteQueryBuilder(
            "localhost", "metric.value", 1234, 5678
        )

        assert qb.build_query().find("target=metric.value") >= 0
        assert qb.build_query().find("from=1234") >= 0
        assert qb.build_query().find("until=5678") >= 0
        assert qb.build_query().find("format=json") >= 0
