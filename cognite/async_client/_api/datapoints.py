import copy
import math
import queue
import threading
from datetime import datetime
from typing import *

import numpy as np
import pandas as pd

from cognite.async_client.concurrency import DatapointsListJob
from cognite.async_client.utils import extends_class, to_list
from cognite.client._api.datapoints import DatapointsAPI, DatapointsFetcher


@extends_class(extends=DatapointsAPI)
class DataPointsAPIExtensions:
    """Extensions to the Datapoints API"""

    def retrieve_async(
        self,
        start: Union[int, str, datetime] = 0,
        end: Union[int, str, datetime] = "now",
        id: Union[int, List[int], Dict[int, Any]] = None,
        external_id: Union[str, List[str], Dict[str, Any]] = None,
        aggregates: Union[str, List[str]] = None,
        granularity: str = None,
        include_outside_points: bool = None,
    ):
        """Asynchronous datapoints retrieval.

        Returns:
            A Job object whose `result` property waits for and returns a DatapointsList with the requested datapoints.
        """
        items, _ = DatapointsFetcher._process_ts_identifiers(id, external_id)
        base = {
            "start": start,
            "end": end,
            "aggregates": aggregates,
            "granularity": granularity,
            "includeOutsidePoints": include_outside_points,
        }
        return self._cognite_client.submit_job(DatapointsListJob([{**base, **item} for item in items], self))

    def retrieve_dataframe_async(
        self,
        start: Union[int, str, datetime],
        end: Union[int, str, datetime],
        aggregates: Union[str, List[str]],
        granularity: str,
        id: Union[int, List[int], Dict[int, Any]] = None,
        external_id: Union[str, List[str], Dict[str, Any]] = None,
    ) -> "pandas.DataFrame":
        """Asynchronous dataframe retrieval.

        Returns:
            A Job object whose `result` property waits for and returns a pandas DataFrame with the requested datapoints.
        """
        job = self.retrieve_async(start, end, id, external_id, aggregates, granularity)
        job.add_callback(lambda dpl: dpl.to_pandas())
        return job
