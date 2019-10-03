import os
import sys

import numpy as np
import pytest

from cognite.async_client import CogniteClient
from cognite.async_client.concurrency import Job

client = CogniteClient(server="greenfield", project="sander")


class ReturnIntJob(Job):
    def __init__(self, n=1):
        super().__init__()
        self.n = n

    def run(self):
        return self.n


class SplittableJob(Job):
    def __init__(self, ns):
        super().__init__()
        self.ns = ns

    def initial_split(self):
        return [ReturnIntJob(n) for n in self.ns]

    def merge(self):
        return self.children


class TestJobQueue:
    def test_single(self):
        r = client.submit_job(ReturnIntJob())
        assert 1 == r.result
        assert client.job_queue.done
        assert client.job_queue.healthy

    def test_many(self):
        jobs = [ReturnIntJob(n) for n in range(100)]
        assert 100 < jobs[-1].priority < 100000000
        rl = client.submit_jobs(jobs)
        assert 100 < rl[-1].priority < 100000000
        for i, r in enumerate(rl):
            assert i == r.result
        assert client.job_queue.done
        assert client.job_queue.healthy

    def test_many_splits(self):
        jobs = [SplittableJob([n, 42]) for n in range(100)]
        rl = client.submit_jobs(jobs)
        for i, r in enumerate(rl):
            assert [i, 42] == r.result
        assert client.job_queue.done
        assert client.job_queue.healthy
