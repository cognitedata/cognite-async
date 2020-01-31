from cognite.async_client.jobs import Job
from cognite.async_client.utils import to_list
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._auxiliary import split_into_chunks


class CreateJob(Job):
    def __init__(self, resources, api_client, upsert=False):
        super().__init__(api_client=api_client)
        self.resources = to_list(resources)
        self.upsert = upsert
        if upsert and any([res.external_id is None for res in self.resources]):
            raise ValueError("can only upsert for objects with external_id")

    def initial_split(self):
        return [
            CreateJob(resources=chunk, api_client=self.api_client, upsert=self.upsert)
            for chunk in split_into_chunks(self.resources, self.api_client._CREATE_LIMIT)
        ]

    def create(self, resources):
        response = self.api_client._post(
            self.api_client._RESOURCE_PATH, {"items": [res.dump(camel_case=True) for res in resources]}
        )
        return self.api_client._LIST_CLASS._load(response.json()["items"])

    def update(self, resources):
        patches = [
            self.api_client._convert_resource_to_patch_object(
                res, self.api_client._LIST_CLASS._UPDATE._get_update_properties()
            )
            for res in resources
        ]
        response = self.api_client._post(self.api_client._RESOURCE_PATH + "/update", {"items": patches})
        return self.api_client._LIST_CLASS._load(response.json()["items"])

    def run(self):
        if self.upsert:
            try:
                created = self.create(self.resources)
                updated = self.api_client._LIST_CLASS([])
            except CogniteAPIError as ex:
                if not ex.duplicated:
                    raise ex
                dups = {res["externalId"] for res in ex.duplicated}
                nondup_res = [res for res in self.resources if res.external_id not in dups]
                created = self.create(nondup_res) if nondup_res else self.api_client._LIST_CLASS([])
                updated = self.update([res for res in self.resources if res.external_id in dups])
            return {"created": created, "updated": updated}
        else:
            return self.create(self.resources)

    def merge(self):
        if not self.upsert:
            return super().merge()
        result = self.children[0]
        for child in self.children[1:]:
            result["created"].extend(child["created"])
            result["updated"].extend(child["updated"])
        return result
