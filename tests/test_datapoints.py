import os
import sys
from datetime import datetime

import numpy as np
import pytest

from cognite.async_client import CogniteClient

client = CogniteClient(server="greenfield", project="sander")


class TestDatapointsJob:
    def test_retrieve_async_aggregates(self):
        dpl_old = client.datapoints.retrieve(
            external_id="ts_1min", start=0, end=datetime(2018, 3, 1), aggregates=["interpolation"], granularity="1m"
        )
        j = client.datapoints.retrieve_async(
            external_id="ts_1min", start=0, end=datetime(2018, 3, 1), aggregates="interpolation", granularity="1m"
        )
        assert len(dpl_old) == len(j.result[0])
        assert dpl_old == j.result[0]

    def test_retrieve_async(self):
        dpl_old = client.datapoints.retrieve(external_id="ts_1min", start=0, end=datetime(2018, 3, 1))
        j = client.datapoints.retrieve_async(external_id="ts_1min", start=0, end=datetime(2018, 3, 1))
        assert len(dpl_old) == len(j.result[0])
        assert dpl_old == j.result[0]

    def test_retrieve_async_inc_outside(self):
        dpl_old = client.datapoints.retrieve(
            external_id="ts_1min", start=0, end=datetime(2018, 3, 1), include_outside_points=True
        )
        j = client.datapoints.retrieve_async(
            external_id="ts_1min", start=0, end=datetime(2018, 3, 1), include_outside_points=True
        )
        assert len(dpl_old) == len(j.result[0])
        assert dpl_old == j.result[0]
