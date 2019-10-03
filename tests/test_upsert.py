import os
import sys
from unittest import mock

import numpy as np
import pytest

from cognite.async_client import CogniteClient
from cognite.client.data_classes import Asset

client = CogniteClient(server="greenfield", project="sander")


class TestUpsert:
    def test_upsert_create(self):
        assets = [Asset(description="delete me", name=str(i)) for i in range(5)]

        r = client.assets.upsert(assets[:2])
        assert 2 == len(r['created'])
        assert 0 == len(r['updated'])

        r = client.assets.upsert(assets)
        assert 3 == len(r['created'])
        assert 2 == len(r['updated'])

        client.assets.delete(id=[a.id for a in r['created'] + r['updated']])

    def test_upsert_async(self):
        assets = [Asset(description="delete me", name=str(i)) for i in range(5)]

        r = client.assets.upsert_async(assets[:2]).result
        assert 2 == len(r['created'])
        assert 0 == len(r['updated'])

        r = client.assets.upsert_async(assets).result
        assert 3 == len(r['created'])
        assert 2 == len(r['updated'])

        client.assets.delete(id=[a.id for a in r['created'] + r['updated']])
