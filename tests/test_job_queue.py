import os
import sys

import numpy as np
import pytest

from cognite.async_client import CogniteClient
from cognite.async_client.concurrency import Job
from cognite.async_client.exceptions import CogniteJobError

client = CogniteClient(server="greenfield", project="sander")


class ReturnIntJob(Job):
    def __init__(self, n=1):
        super().__init__()
        self.n = n

    def run(self):
        if self.n == 123456789:
            foo
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

    def test_single_failing(self):
        r = client.submit_job(ReturnIntJob(123456789))
        with pytest.raises(CogniteJobError) as exinfo:
            r.result
        assert 1 == len(exinfo.value)
        assert "foo" in str(exinfo.value)
        assert client.job_queue.done
        assert client.job_queue.healthy

    def test_multi_failing(self):
        r = client.submit_job(SplittableJob([1, 2, 123456789, 123456789]))
        with pytest.raises(CogniteJobError) as exinfo:
            r.result
        assert "2 Exceptions" in str(exinfo.value)
        assert "foo" in str(exinfo.value)
        assert 2 == len(exinfo.value)
        assert client.job_queue.done
        assert client.job_queue.healthy
