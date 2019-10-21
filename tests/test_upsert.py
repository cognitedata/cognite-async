import os
import sys
from unittest import mock

import numpy as np
import pytest

from cognite.async_client import CogniteClient
from cognite.client.data_classes import Asset

client = CogniteClient(server="greenfield", project="sander")


@pytest.fixture
def example_assets():
    assets = [Asset(description="delete me", name=str(i), external_id=str(i)) for i in range(5)]
    for a in assets:
        try:
            client.assets.delete(external_id=a.external_id, recursive=True)
        except:
            pass
    yield assets


class TestUpsert:
    def test_upsert_create(self, example_assets):
        r1 = client.assets.upsert(example_assets[:2])
        assert 2 == len(r1["created"])
        assert 0 == len(r1["updated"])

        r2 = client.assets.upsert(example_assets)
        assert 3 == len(r2["created"])
        assert 2 == len(r2["updated"])

    def test_upsert_async(self, example_assets):
        r1 = client.assets.upsert_async(example_assets[:2]).result
        assert 2 == len(r1["created"])
        assert 0 == len(r1["updated"])

        r2 = client.assets.upsert_async(example_assets).result
        assert 3 == len(r2["created"])
        assert 2 == len(r2["updated"])
