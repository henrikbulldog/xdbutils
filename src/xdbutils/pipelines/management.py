import time
import urllib
import requests

class DLTPipelineManager():
    """ Delta Live Tables Pipeline Manager """

    def __init__(
        self,
        spark,
        dbutils,
        source_system,
        entity,
        catalog,
        tags = None,
        continuous_workflow = False,
        databricks_token = None,
        databricks_host = None,
        source_path = None,
        create_or_update = True,
        ):

        self.spark = spark
        self.dbutils = dbutils
        self.source_system = source_system
        self.source_path = source_path
        self.entity = entity
        self.catalog = catalog
        self.continuous_workflow = continuous_workflow
        self.tags = tags
        self.databricks_token = databricks_token
        self.databricks_host = databricks_host

        try:
            if not self.databricks_host:
                self.databricks_host = spark.conf.get("spark.databricks.workspaceUrl")
        except Exception as exc: # pylint: disable=broad-exception-caught
            print("Could not get databricks host from Spark configuration,", 
                "please specify databricks_host.", exc)
            return

        try:
            if not self.source_path:
                self.source_path = (
                    dbutils.notebook.entry_point.getDbutils()
                    .notebook().getContext()
                    .notebookPath().get()
                )
        except Exception as exc: # pylint: disable=broad-exception-caught
            print("Could not get source path from notebook context,",
                "please specify source_path.", exc)
            return


        if not self.tags:
            self.tags = {}
        self.tags["Source system"] = self.source_system
        self.tags["Entity"] = self.entity

        if create_or_update:
            self.create_or_update()

    def create_or_update(
        self,
        ):
        """ Create Delta Live Tables Workflow """

        try:
            workflow_settings = self.__compose_settings(
                continuous_workflow=self.continuous_workflow,
                )

            pipeline_id = self.get_id()

            if pipeline_id:
                print(f"Updating pipeline {self.source_system}-{self.entity}")
                self.__update(
                    pipeline_id=pipeline_id,
                    workflow_settings=workflow_settings,
                    )
            else:
                print(f"Creating pipeline {self.source_system}-{self.entity}")
                self.__create(
                    workflow_settings=workflow_settings,
                    )
        except Exception as exc: # pylint: disable=broad-exception-caught
            # Cannot get information from notebook context, give up
            print("Could not create DLT workflow.", exc)
            return

    def get_id(
        self,
        ):
        name = f"{self.source_system}-{self.entity}"
        params = urllib.parse.urlencode(
            {"filter": f"name LIKE '{name}'"},
            quote_via=urllib.parse.quote)

        response = requests.get(
            url=f"https://{self.databricks_host}/api/2.0/pipelines",
            params=params,
            headers={"Authorization": f"Bearer {self.databricks_token}"},
            timeout=60
            )

        if response.status_code == 200:
            payload = response.json()
            if "statuses" in payload.keys():
                if len(payload["statuses"]) == 1:
                    return payload["statuses"][0]["pipeline_id"]

        return None

    def start(self):
        print(f"Starting pipeline {self.source_system}-{self.entity}")
        pipeline_id = self.get_id()
        self.__refresh(pipeline_id=pipeline_id)

    def stop(self):
        print(f"Stopping pipeline {self.source_system}-{self.entity}")
        pipeline_id = self.get_id()
        self.__stop(pipeline_id=pipeline_id)

    def __wait_until_state(self, pipeline_id, states):
        update_id = self.__get_latest_update(
            pipeline_id=pipeline_id,
        )
        if not update_id:
            print(f"Pipeline {self.source_system}-{self.entity}: latest update not found")
            return

        for x in range(60):
            time.sleep(10)
            progress = self.__get_progress(
                pipeline_id=pipeline_id,
                update_id=update_id,
            )
            print(f"{self.source_system}-{self.entity}, update_id: {update_id}, progress: {progress}")
            if progress:
                assert progress.lower() != "failed", f"Pipeline {self.source_system}-{self.entity}: update failed"
                if progress.lower() == "canceled":
                    break
                if progress.lower() in states:
                    break

    def __compose_settings(
        self,
        continuous_workflow,
        ):

        settings = {
            "name": f"{self.source_system}-{self.entity}",
            "edition": "Advanced",
            "development": True,
            "clusters": [
                {
                "label": "default",
                "autoscale": {
                    "min_workers": 1,
                    "max_workers": 2,
                    "mode": "ENHANCED"
                }
                }
            ],
            "channel": "PREVIEW",
            "libraries": [
                {
                "notebook": {
                    "path": self.source_path
                }
                }
            ],
            "catalog": self.catalog,
            "target": self.source_system,
            "configuration": {
                "pipelines.enableTrackHistory": "true"
            },
            "continuous": continuous_workflow
        }

        settings["configuration"].update(self.tags)

        return settings

    def __update(
        self,
        pipeline_id,
        workflow_settings,
        ):

        response = requests.put(
            url=f"https://{self.databricks_host}/api/2.0/pipelines/{pipeline_id}",
            json=workflow_settings,
            headers={"Authorization": f"Bearer {self.databricks_token}"},
            timeout=60
            )

        response.raise_for_status()

        if self.continuous_workflow:
            self.__wait_until_state(pipeline_id=pipeline_id, states=["running"])

    def __create(
        self,
        workflow_settings,
        ):

        response = requests.post(
            url=f"https://{self.databricks_host}/api/2.0/pipelines",
            json=workflow_settings,
            headers={"Authorization": f"Bearer {self.databricks_token}"},
            timeout=60
            )

        response.raise_for_status()

        if self.continuous_workflow:
            self.__wait_until_state(pipeline_id=self.get_id(), states=["running"])

    def __get_latest_update(
        self,
        pipeline_id,
    ):
        params = urllib.parse.urlencode(
            {
                "order_by": "timestamp desc",
                "max_results": 100,
            },
            quote_via=urllib.parse.quote,
        )

        response = requests.get(
            url=f"https://{self.databricks_host}/api/2.0/pipelines/{pipeline_id}/events",
            params=params,
            headers={"Authorization": f"Bearer {self.databricks_token}"},
            timeout=60,
        )

        payload = response.json()

        if not "events" in payload:
            return None

        updates = [
            e["origin"]["update_id"]
            for e in payload["events"]
            if e["event_type"] == "create_update"
        ]

        if not updates:
            return None

        return next(iter(updates), None)

    def __get_datasets(
        self,
        pipeline_id,
        update_id,
    ):
        params = urllib.parse.urlencode(
            {
                "order_by": "timestamp desc",
                "max_results": 100,
            },
            quote_via=urllib.parse.quote,
        )

        response = requests.get(
            url=f"https://{self.databricks_host}/api/2.0/pipelines/{pipeline_id}/events",
            params=params,
            headers={"Authorization": f"Bearer {self.databricks_token}"},
            timeout=60,
        )

        payload = response.json()

        return [
            e["details"]["flow_definition"]["output_dataset"]
            for e in payload["events"]
            if e["event_type"] == "flow_definition"
            and e["origin"]["update_id"] == update_id
        ]

    def __refresh(
        self,
        pipeline_id,
        full_refresh=False,
        refresh_selection=[],
        full_refresh_selection=[],
    ):
        refresh_settings = {
            "full_refresh": full_refresh,
            "refresh_selection": refresh_selection,
            "full_refresh_selection": full_refresh_selection,
        }

        response = requests.post(
            url=f"https://{self.databricks_host}/api/2.0/pipelines/{pipeline_id}/updates",
            json=refresh_settings,
            headers={"Authorization": f"Bearer {self.databricks_token}"},
            timeout=60,
        )

        response.raise_for_status()

        if self.continuous_workflow:
            states = ["running"]
        else:
            states = ["completed"]
        self.__wait_until_state(pipeline_id=pipeline_id, states=states)

    def __get_progress(
        self,
        pipeline_id,
        update_id,
    ):
        params = urllib.parse.urlencode(
            {
                "order_by": "timestamp desc",
                "max_results": 100,
            },
            quote_via=urllib.parse.quote,
        )

        response = requests.get(
            url=f"https://{self.databricks_host}/api/2.0/pipelines/{pipeline_id}/events",
            params=params,
            headers={"Authorization": f"Bearer {self.databricks_token}"},
            timeout=60,
        )

        payload = response.json()

        if not "events" in payload:
            return None

        updates = [
            e["details"]["update_progress"]["state"]
            for e in payload["events"]
            if e["event_type"] == "update_progress"
            and e["origin"]["update_id"] == update_id
        ]

        if not updates:
            return None

        return next(iter(updates), None)

    def __stop(self, pipeline_id):
        response = requests.post(
            url=f"https://{self.databricks_host}/api/2.0/pipelines/{pipeline_id}/stop",
            headers={"Authorization": f"Bearer {self.databricks_token}"},
            timeout=60,
        )

        response.raise_for_status()

        self.__wait_until_state(pipeline_id=pipeline_id, states=[])
