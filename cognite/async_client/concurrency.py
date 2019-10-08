import collections
import copy
import math
import queue
import sys
import threading
from concurrent.futures import Future

import pandas as pd

from cognite.async_client.utils import to_list
from cognite.client.data_classes import Datapoints, DatapointsList
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils import timestamp_to_ms
from cognite.client.utils._auxiliary import split_into_chunks
from cognite.client.utils._concurrency import collect_exc_info_and_raise
from cognite.client.utils._time import granularity_to_ms, granularity_unit_to_ms


class JobQueue:
    def __init__(self, num_workers):
        self.job_queue = queue.PriorityQueue()
        self.num_workers = num_workers
        self._idle = [True] * num_workers
        self.any_idle = True
        self._failed_jobs = []
        self._threadpool = [
            threading.Thread(target=self._run_jobs, args=[tid], daemon=True) for tid in range(self.num_workers)
        ]
        for t in self._threadpool:
            t.start()

    @property
    def done(self):
        return self.job_queue.empty() and all(self._idle)

    @property
    def healthy(self):
        return not any([isinstance(t, Exception) for t in self._threadpool])

    def submit(self, jobs, priority=None):
        for job in to_list(jobs):
            for subjob in job._initial_split():
                subjob.priority = priority or subjob.priority or 1e9
                self.job_queue.put(subjob)
        return jobs

    def _run_jobs(self, tid):
        try:
            while True:
                try:
                    self._idle[tid] = False
                    job = self.job_queue.get(block=False)
                except queue.Empty:
                    self._idle[tid] = True
                    self.any_idle = True
                    job = self.job_queue.get(block=True)
                    self._idle[tid] = False
                    self.any_idle = any(self._idle)
                if self.any_idle and job.splittable() and self.job_queue.empty():
                    subjobs = job._split(nparts=sum(self._idle) + 1)
                    if len(subjobs) != 1:
                        self.submit(subjobs, job.priority)
                        continue
                    else:
                        job = subjobs[0]  # prevent infinisplit when splitting gives 1 part
                continued_job = job._run_and_store()
                if continued_job:
                    self.submit(continued_job)
        except Exception as e:
            print("Exception in Job Queue. Please report this on slack or github. Exception: ", e, "\nTraceback:")
            import traceback

            traceback.print_tb(sys.exc_info()[2])
            self._threadpool[tid] = e.with_traceback(sys.exc_info()[2])

    def __str__(self):
        exc = [t for t in self._threadpool if isinstance(t, Exception)]
        if exc:
            s = "MAJOR ERROR IN JOB WORKER. {} workers died with exceptions: {}".format(len(exc), exc)
        else:
            s = "queue {}, {} workers alive, {} workers idle".format(
                "empty" if self.job_queue.empty() else "not empty",
                sum([t.is_alive() for t in self._threadpool]),
                sum(self._idle),
            )
        return s

    def failed_jobs(self):
        return self._failed_jobs


class Job:
    PRIORITY_COUNTER = 1

    def __init__(self, api_client=None):
        self._result = Future()
        self.api_client = api_client
        self.parent = None
        self.children = None
        self.priority = self.PRIORITY_COUNTER
        Job.PRIORITY_COUNTER += 1
        self.callbacks = []
        self.callback_lock = threading.Lock()

    def __lt__(self, other):
        return self.priority < other.priority

    def process_callbacks(self,result):
        for cb in self.callbacks:
            if isinstance(result, list) and result and isinstance(result[0], Exception):
                ex_list = result
            else:
                ex_list = []
            try:
                cb_ret = cb(result)
            except Exception as e:
                ex_list.append(e)
                cb_ret = ex_list
            result = cb_ret if cb_ret is not None else result
        self.callbacks = []
        return result

    def add_callback(self,callback):
        """Add a callback to be called with the result when the job is done.
        Callbacks will be called in order, and can modify the result if returning a value other than None.
        When setting on a job that is done already, will callback immediately (and synchronously).
        Note that in the case of exception(s) in the job or previous callbacks, a list of exception objects will be passed instead of a result."""
        with self.callback_lock:
            self.callbacks.append(callback)
            if self._result.done(): # trying to set a callback on a job that's done
                self._result.set_result(self.process_callbacks(self._result.result()))

    @property
    def result(self):
        """waits for job to finish and return the result, or raise any exceptions that occurred"""
        res = self._result.result()
        if isinstance(res, list) and res and isinstance(res[0], Exception):
            collect_exc_info_and_raise(res)
        return res

    def _set_result(self, result):
        if self.parent:
            self._result.set_result("child_job_finished")  # should not duplicate data here, all goes to parent
            self.parent._merge_child(result, self.child_index)
        else:
            with self.callback_lock:
                result = self.process_callbacks(result)
                self._result.set_result(result)

    def splittable(self):
        return False

    def split(self, nparts):
        """splits job into sub-jobs when the queue is too empty. only called when self.splittable()"""
        raise Exception("Job is not splittable")

    def run(self):
        raise NotImplementedError("Abstract base class method called")

    def initial_split(self):
        """splits job into initial batches."""
        return [self]

    def merge(self):
        """merges results and updates future when all child jobs are done"""
        result = self.children[0]
        for child in self.children[1:]:
            result.extend(child)  # should work for resource lists etc
        return result

    def _handle_split(self, subjobs):
        if subjobs and not (len(subjobs) == 1 and subjobs[0] == self):  # if split
            self.children = subjobs
            self.merge_lock = threading.Lock()
            for child_ix, subjob in enumerate(self.children):
                subjob.child_index = child_ix
                subjob.parent = self
        return subjobs

    def _split(self, nparts):
        return self._handle_split(self.split(nparts))

    def _initial_split(self):
        return self._handle_split(self.initial_split())

    def _merge_child(self, result, child_index):
        with self.merge_lock:
            self.children[child_index] = result
            if all([not isinstance(j, Job) for j in self.children]):
                exc = [e for e in self.children if isinstance(e, list) and e and isinstance(e[0], Exception)]
                self._set_result(sum(exc, []) if exc else self.merge())

    def _run_and_store(self):
        try:
            result = self.run()
            if isinstance(result, Job):
                return result  # continue Job instead of storing
        except Exception as e:
            result = [e]
        self._set_result(result)


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


class CreateJob(Job):
    def __init__(self, resources, api_client, upsert=False):
        super().__init__(api_client=api_client)
        self.resources = to_list(resources)
        self.upsert = upsert
        if upsert and any([res.external_id is None for res in self.resources]):
            raise ValueError("can only upsert for objects with external_id")

    def initial_split(self):
        return [
            CreateJob(resources=chunk, api_client=self.api_client, upsert=self.upsert)
            for chunk in split_into_chunks(self.resources, self.api_client._CREATE_LIMIT)
        ]

    def create(self, resources):
        response = self.api_client._post(
            self.api_client._RESOURCE_PATH, {"items": [res.dump(camel_case=True) for res in resources]}
        )
        return self.api_client._LIST_CLASS._load(response.json()["items"])

    def update(self, resources):
        patches = [
            self.api_client._convert_resource_to_patch_object(
                res, self.api_client._LIST_CLASS._UPDATE._get_update_properties()
            )
            for res in resources
        ]
        response = self.api_client._post(self.api_client._RESOURCE_PATH + "/update", {"items": patches})
        return self.api_client._LIST_CLASS._load(response.json()["items"])

    def run(self):
        if self.upsert:
            try:
                created = self.create(self.resources)
                updated = self.api_client._LIST_CLASS([])
            except CogniteAPIError as ex:
                if not ex.duplicated:
                    raise ex
                dups = {res["externalId"] for res in ex.duplicated}
                nondup_res = [res for res in self.resources if res.external_id not in dups]
                created = self.create(nondup_res) if nondup_res else self.api_client._LIST_CLASS([])
                updated = self.update([res for res in self.resources if res.external_id in dups])
            return {"created": created, "updated": updated}
        else:
            return self.create(self.resources)

    def merge(self):
        if not self.upsert:
            return super().merge()
        result = self.children[0]
        for child in self.children[1:]:
            result["created"].extend(child["created"])
            result["updated"].extend(child["updated"])
        return result
