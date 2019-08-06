import json
import requests


class GraphiteQuery:
    def __init__(self, graphite_render_url, target, start_time, end_time=None):
        self.base_url = graphite_render_url
        self.target = target
        self.start_time = start_time
        self.end_time = end_time
        self.query = self._build_query()

    def get_metrics(self):
        r = requests.get(self.query)
        if r.status_code == 200:
            return json.loads(r.text)
        else:
            raise Exception("Status code {} is not 200".format(r.status_code))

    def _build_query(self):
        q = self.base_url
        q = q + "?target={}".format(self.target)
        q = q + "&from={}".format(self.start_time)
        if self.end_time is not None:
            q = q + "&until={}".format(self.end_time)
        q = q + "&format=json"
        return q
