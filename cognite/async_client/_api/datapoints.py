import copy
import math
import queue
import threading
from datetime import datetime
from typing import *

import numpy as np
import pandas as pd

from cognite.client._api.datapoints import DatapointsAPI, DatapointsFetcher
from cognite.async_client.concurrency import DatapointsListJob
from cognite.async_client.utils import to_list, extends_class

@extends_class(extends=DatapointsAPI)
class DataPointsAPIExtensions:
    """Extensions to the Datapoints API"""

    def retrieve_async(
        self,
        start: Union[int, str, datetime] = 0,
        end: Union[int, str, datetime] = "now",
        id: Union[int, List[int], Dict[str, Union[int, List[str]]], List[Dict[str, Union[int, List[str]]]]] = None,
        external_id: Union[
            str, List[str], Dict[str, Union[int, List[str]]], List[Dict[str, Union[int, List[str]]]]
        ] = None,
        aggregates: Union[str, List[str]] = None,
        granularity: str = None,
        include_outside_points: bool = None,
    ):
        """Asynchronous datapoints retrieval.

        Returns:
            A Job object whose `result` property waits for and returns a DatapointsList with the requested datapoints.
        """
        items,_ = DatapointsFetcher._process_ts_identifiers(id, external_id)
        base = {
            "start": start,
            "end": end,
            "aggregates": aggregates,
            "granularity": granularity,
            "includeOutsidePoints": include_outside_points,
        }
        return self._cognite_client.submit_job(DatapointsListJob([{**base, **item} for item in items], self))
