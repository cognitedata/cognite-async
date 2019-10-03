from cognite.async_client.utils import extends_class
from cognite.client._api.assets import AssetsAPI


@extends_class(extends=AssetsAPI)
class AssetsAPIExtensions:
    """Extensions to AssetsAPI"""
    pass
