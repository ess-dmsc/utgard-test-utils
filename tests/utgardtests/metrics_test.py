import pytest
import requests
import utgardtests.metrics as metrics


class GoodResponseStub:
    status_code = 200
    text = '[{"target": "metric.value", "tags": {"name": "metric.value"}, "datapoints": [[0.1, 10], [0.5, 20], [1.2, 30]]}]'


class BadResponseStub:
    status_code = 404


class TestGraphiteQuery:
    def build_query_spy(self):
        pass

    def requests_get_good_stub(self, query):
        return GoodResponseStub()

    def requests_get_bad_stub(self, query):
        return BadResponseStub()

    def test_query(self):
        client = metrics.GraphiteQuery("localhost", "metric.value", 1234)

        assert client.query.find("target=metric.value") >= 0
        assert client.query.find("from=1234") >= 0
        assert client.query.find("format=json") >= 0

    def test_good_query(self, monkeypatch):
        monkeypatch.setattr(requests, "get", self.requests_get_good_stub)

        client = metrics.GraphiteQuery("localhost", "metric.value", 1234)
        m = client.get_metrics()

        assert len(m) == 1
        assert m[0]['target'] == 'metric.value'
        assert len(m[0]['datapoints']) == 3
        assert [0.5, 20] in m[0]['datapoints']

    def test_bad_query(self, monkeypatch):
        monkeypatch.setattr(requests, "get", self.requests_get_bad_stub)
        client = metrics.GraphiteQuery("localhost", "metric.value", 1234)

        with pytest.raises(Exception):
            client.get_metrics()
