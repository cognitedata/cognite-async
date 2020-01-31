import copy
import math

from cognite.async_client.jobs import Job
from cognite.async_client.utils import to_list
from cognite.client.data_classes import Datapoints, DatapointsList
from cognite.client.utils import timestamp_to_ms
from cognite.client.utils._time import granularity_to_ms, granularity_unit_to_ms


class DatapointsListJob(Job):
    def __init__(self, ts_items: list, api_client):
        super().__init__(api_client=api_client)
        self.ts_items = ts_items

    def initial_split(self):
        return [DatapointsJob(ts_item, self.api_client) for ts_item in self.ts_items]

    def merge(self):
        result = DatapointsList([], cognite_client=self.api_client)
        for child_res in self.children:
            result.append(child_res)  # expected fields in case of aggregates
        return result


class DatapointsJob(Job):
    def __init__(self, query, api_client):
        super().__init__(api_client=api_client)
        self.query = query
        self.query["start"] = timestamp_to_ms(self.query["start"])
        self.query["end"] = timestamp_to_ms(self.query["end"])
        if self.query.get("aggregates"):
            self.query["aggregates"] = to_list(self.query["aggregates"])
        self.aggregate_job = bool(self.query.get("aggregates"))
        if self.aggregate_job:
            self.query["start"] = self._align_with_granularity_unit(self.query["start"], self.query["granularity"])
            self.query["end"] = self._align_with_granularity_unit(self.query["end"], self.query["granularity"])
        self.retrieved_data = Datapoints()

    def __repr__(self):
        return f"<DataPointsJob query={self.query.__repr__()}>"

    def splittable(self):
        return not self.query.get("limit") and self.query["start"] > 0  # don't split jobs at t=0

    @property
    def granularity(self):
        return granularity_to_ms(self.query.get("granularity")) if self.aggregate_job else 1

    @property
    def limit(self):
        return self.api_client._DPS_LIMIT_AGG if self.aggregate_job else self.api_client._DPS_LIMIT

    def split(self, nparts):
        if nparts == 1:
            return [self]
        else:
            spacing = self.granularity
            npt = (self.query["end"] - self.query["start"]) / spacing
            nparts = min(nparts, math.ceil(npt / self.limit))  # no more parts than DPS_LIMIT per part
            if npt < 0:
                self.splittable = False
                return [self]
            chunk_size = math.ceil(npt / nparts) * spacing
            new_queries = []
            for i in range(nparts):
                qc = copy.copy(self.query)
                qc["start"] = self.query["start"] + i * chunk_size
                qc["end"] = self.query["start"] + (i + 1) * chunk_size
                new_queries.append(qc)
            new_queries[-1]["end"] = self.query["end"]
            return [DatapointsJob(q, self.api_client) for q in new_queries]

    def merge(self):
        r = self.retrieved_data  # could have some retrievals followed by a split
        outside_points = self.query.get("includeOutsidePoints")
        for child_res in self.children:
            if outside_points:
                if len(child_res) >= 2 and len(r) >= 2 and child_res.timestamp[:2] == r.timestamp[-2:]:
                    child_res = child_res[2:]  # strip duplicated include outside points
            r._extend(child_res)
        return r

    def run(self):
        payload = {"items": [self.query], "limit": self.limit}
        result = self.api_client._post(self.api_client._RESOURCE_PATH + "/list", json=payload)
        data = result.json()["items"][0]
        retrieved_inside_range = len(data["datapoints"])
        at_end = not data["datapoints"] or data["datapoints"][-1]["timestamp"] + self.granularity >= self.query["end"]
        if self.query.get("includeOutsidePoints") and data["datapoints"]:
            if data["datapoints"][0]["timestamp"] < self.query["start"]:
                retrieved_inside_range -= 1
                if self.retrieved_data:  # second page, ignore point before
                    data["datapoints"] = data["datapoints"][1:]
            if data["datapoints"] and data["datapoints"][-1]["timestamp"] >= self.query["end"]:
                retrieved_inside_range -= 1
                # we still need to paginate, so point after is duplicate here (and will mess up start)
                at_end = data["datapoints"][-2]["timestamp"] + self.granularity >= self.query["end"]
                if retrieved_inside_range == self.limit and not at_end:
                    data["datapoints"] = data["datapoints"][:-1]
        self.retrieved_data._extend(Datapoints._load(data, expected_fields=self.query.get("aggregates", ["value"])))
        if retrieved_inside_range == self.limit and not at_end:
            self.query["start"] = data["datapoints"][-1]["timestamp"] + self.granularity
            return self  # continue job
        else:
            return self.retrieved_data  # done

    @staticmethod
    def _align_with_granularity_unit(ts: int, granularity: str):
        gms = granularity_unit_to_ms(granularity)
        if ts % gms == 0:
            return ts
        return ts - (ts % gms) + gms


class CountDatapointsJob(DatapointsJob):
    def __init__(self, time_series, start, end, api_client):
        self.time_series = time_series
        if self.time_series.is_string:
            query = {"id": time_series.id, "start": start, "end": end}
        else:
            query = {"id": time_series.id, "start": start, "end": end, "aggregates": ["count"], "granularity": "10d"}
        super().__init__(query=query, api_client=api_client)
        self.count = 0

    def split(self, nparts):
        return [
            CountDatapointsJob(self.time_series, j.query["start"], j.query["end"], api_client=self.api_client)
            for j in super().split(nparts)
        ]

    def run(self):
        r = super().run()
        if self.time_series.is_string:
            jcount = len(self.retrieved_data)
        else:
            jcount = sum(self.retrieved_data.count or [])
        self.retrieved_data = Datapoints()
        self.count += jcount
        if isinstance(r, Job):
            return r
        else:
            return self.count

    def merge(self):
        return self.count + sum(self.children)
