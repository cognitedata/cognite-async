from cognite.async_client.concurrency import CreateJob
from cognite.async_client.utils import extends_class
from cognite.client._api_client import APIClient
from cognite.client.exceptions import *


@extends_class(extends=APIClient)
class ApiClientExtensions:
    """Extensions to the cognite.client.ApiClient base class"""

    def create_async(self, resources):
        """Create resources (assets/events/time series/etc) asynchronously.

        Args:
            resources (Union[CogniteResource,List[CogniteResource]]): List of resources to be created.

        Returns:
            Future[CogniteResourceList]: future for the created resources. Unlike the normal create function, the return type is always a CogniteResourceList.
        """
        return self._cognite_client.submit_job(CreateJob(resources, api_client=self))

    def upsert(self, resources):
        """Creates objects and updates if they already exist.

        Args:
            resources (Union[CogniteResource,List[CogniteResource]]): List of resources to be created.

        Returns:
            Dict[str,CogniteResourceList]: dictionary of {"created": list of created resources, "updated": list of updated resources}

        """
        return self.upsert_async(resources).result

    def upsert_async(self, resources):
        """Creates objects and updates if they already exist.

        Args:
            resources (Union[CogniteResource,List[CogniteResource]]): List of resources to be created.

        Returns:
            Dict[str,CogniteResourceList]: dictionary of {"created": list of created resources, "updated": list of updated resources}

        """
        return self._cognite_client.submit_job(CreateJob(resources, api_client=self, upsert=True))
