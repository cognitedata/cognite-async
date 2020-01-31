import queue
import sys
import threading
import traceback

from cognite.async_client.jobs import CountDatapointsJob, CreateJob, DatapointsJob, DatapointsListJob, Job
from cognite.async_client.utils import to_list


class JobQueue:
    def __init__(self, num_workers):
        self.job_queue = queue.PriorityQueue()
        self.num_workers = num_workers
        self._idle = [True] * num_workers
        self.any_idle = True
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
            print(
                "Exception in Job Queue. Please report this on slack or github. Exception: ",
                e,
                "\nTraceback:",
                file=sys.stderr,
            )
            traceback.print_tb(sys.exc_info()[2], file=sys.stderr)
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
