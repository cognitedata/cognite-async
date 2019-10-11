import os
import sys
from unittest import mock

import numpy as np
import pytest

from cognite.async_client import CogniteClient
from cognite.async_client.concurrency import Job
from cognite.client.data_classes import Asset

client = CogniteClient(server="greenfield", project="sander")

@pytest.fixture
def post_spy():
    with mock.patch.object(client.assets, "_post", wraps=client.assets._post) as _:
        yield


class TestCreateJobs:
    def test_multi_create(self,post_spy):
        client.assets._CREATE_LIMIT = 2
        r = client.assets.create_async([Asset(description="delete me", name=str(i)) for i in range(5)])
        al = r.result
        client.assets.delete(id=[a.id for a in al])
        assert 5 == len(al)
        assert ["0", "1", "2", "3", "4"] == [a.name for a in al]
        assert {"delete me"} == set([a.description for a in al])
        assert 3 + 1 == client.assets._post.call_count
