import threading
from concurrent.futures import Future

from cognite.async_client.exceptions import CogniteJobError


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

    def process_callbacks(self, result):
        for cb in self.callbacks:
            if isinstance(result, CogniteJobError):
                ex_list = result
            else:
                ex_list = CogniteJobError()
            try:
                cb_ret = cb(result)
            except Exception as e:  # not expecting a JobError / multiexception here since it's user code
                ex_list.append(e)
                cb_ret = ex_list
            result = cb_ret if cb_ret is not None else result
        self.callbacks = []
        return result

    def add_callback(self, callback):
        """Add a callback to be called with the result when the job is done.
        Callbacks will be called in order, and can modify the result if returning a value other than None.
        When setting on a job that is done already, will callback immediately (and synchronously).
        Note that in the case of exception(s) in the job or previous callbacks, a list of exception objects will be passed instead of a result."""
        with self.callback_lock:
            self.callbacks.append(callback)
            if self._result.done():  # trying to set a callback on a job that's done
                self._result.set_result(self.process_callbacks(self._result.result()))

    @property
    def result(self):
        """waits for job to finish and return the result, or raise any exceptions that occurred"""
        res = self._result.result()
        if isinstance(res, CogniteJobError):
            raise res  # collect_exc_info_and_raise(res)
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
        raise NotImplementedError("The `run` method of a Job needs to be implemented.")

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
            if all([not isinstance(j, Job) for j in self.children]):  # done with all sub-jobs
                exc = [e for e in self.children if isinstance(e, CogniteJobError)]
                self._set_result(sum(exc, CogniteJobError()) if exc else self.merge())

    def _run_and_store(self):
        try:
            result = self.run()
            if isinstance(result, Job):
                return result  # continue Job instead of storing
        except Exception as e:
            result = CogniteJobError([e])
        self._set_result(result)
