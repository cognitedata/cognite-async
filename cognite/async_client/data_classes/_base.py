import copy

from cognite.async_client.utils import extends_class, to_list
from cognite.client.data_classes._base import CogniteResource


@extends_class(extends=CogniteResource)
class CogniteResourceExtensions:
    """extensions to the base CogniteResource class"""

    def insertable_copy(obj):
        """copy of a resource with fields removed that are invalid for create requests"""
        obj = copy.copy(obj)
        obj.id = None
        obj.created_time = None
        obj.last_updated_time = None
        obj.root_id = None
        return obj
