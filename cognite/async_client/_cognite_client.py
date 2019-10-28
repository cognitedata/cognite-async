import os

import cognite.async_client._api  # run extensions
import cognite.async_client._api_client  # run extensions
import cognite.async_client.data_classes._base  # run extensions
from cognite.async_client.concurrency import JobQueue
from cognite.client.experimental import CogniteClient as Client


class CogniteClient(Client):
    """Initializes cognite client, with experimental and async extensions.

    Args:
        * api_key (str): Your api key. If not given, looks for it in environment variables COGNITE_API_KEY and [PROJECT]_API_KEY
        * server (str): Sets base_url to https://[server].cognitedata.com, e.g. server=greenfield.
        * max_workers_async (int): Maximum number of worker threads for the asynchronous job queue. Defaults to max_workers (10).
        * `**kwargs`: other arguments are passed to the SDK.
    """

    def __init__(self, server=None, max_workers_async=None, **kwargs):
        if "base_url" not in kwargs and server is not None:
            kwargs["base_url"] = "https://" + server + ".cognitedata.com"

        if "client_name" not in kwargs and not os.environ.get("COGNITE_CLIENT_NAME"):
            kwargs["client_name"] = "cognite async sdk"

        if "api_key" not in kwargs and not os.environ.get("COGNITE_API_KEY") and "project" in kwargs:
            key = kwargs["project"].upper().replace("-", "_") + "_API_KEY"
            if os.environ.get(key):
                kwargs["api_key"] = os.environ[key]
            else:
                print(os.environ)
                raise ValueError("Did not find api key variable", key)
        if "max_workers" not in kwargs:
            kwargs["max_workers"] = 25
        super().__init__(**kwargs)
        self.job_queue = JobQueue(max_workers_async or self.config.max_workers)

    def submit_jobs(self, jobs):
        return self.job_queue.submit(jobs)

    def submit_job(self, job):
        return self.submit_jobs([job])[0]
