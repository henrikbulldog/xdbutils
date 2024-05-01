""" Delta Live Tables Pipeline Manager """
import time
import urllib
import requests
from pyspark.sql import SparkSession

class DLTPipelineManager():
    """ Delta Live Tables Pipeline Manager """

    def __init__(
        self,
        spark: SparkSession,
        dbutils,
        source_system: str,
        source_class: str,
        catalog: str,
        name: str = None,
        tags: dict = None,
        continuous_workflow: bool = False,
        serverless: bool = False,
        databricks_token: str = None,
        databricks_host: str = None,
        source_path: str = None,
        ):

        self.spark = spark
        self.dbutils = dbutils
        self.source_system = source_system
        self.source_path = source_path
        self.source_class = source_class
        self.catalog = catalog
        self.name = name
        self.continuous_workflow = continuous_workflow
        self.serverless = serverless
        self.tags = tags
        self.databricks_token = databricks_token
        self.databricks_host = databricks_host

        if not self.name:
            self.name = f"{self.source_system}-{self.source_class}"

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
        self.tags["source_class"] = self.source_class


    def help(self):
        """Help"""
        print("This module manages a DLT pipeline")
        print("create_or_update() -> Creates or updates a DLT pipeline")
        print("start() -> Starts a DLT pipeline")
        print("stop() -> Stops a DLT pipeline")


    def create_or_update(
        self,
        ):
        """Creates or updates a DLT pipeline"""
        try:
            workflow_settings = self._compose_settings(
                continuous_workflow=self.continuous_workflow,
                )

            pipeline_id = self._get_id()

            if pipeline_id:
                print(f"Updating pipeline {self.name}")
                self._update(
                    pipeline_id=pipeline_id,
                    workflow_settings=workflow_settings,
                    )
            else:
                print(f"Creating pipeline {self.name}")
                self._create(
                    workflow_settings=workflow_settings,
                    )
        except Exception as exc: # pylint: disable=broad-exception-caught
            # Cannot get information from notebook context, give up
            print("Could not create DLT workflow.", exc)
            return


    def start(self):
        """Starts a DLT pipeline"""
        print(f"Starting pipeline {self.name}")
        pipeline_id = self._get_id()
        self._refresh(pipeline_id=pipeline_id)


    def stop(self):
        """Stops a DLT pipeline"""
        print(f"Stopping pipeline {self.name}")
        pipeline_id = self._get_id()
        self._stop(pipeline_id=pipeline_id)


    def _get_id(
        self,
        ):
        params = urllib.parse.urlencode(
            {"filter": f"name LIKE '{self.name}'"},
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


    def _get_latest_update(
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


    def _get_datasets(
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


    def _wait_until_state(self, pipeline_id, states):
        update_id = self._get_latest_update(
            pipeline_id=pipeline_id,
        )
        if not update_id:
            print(f"Pipeline {self.name}: latest update not found")
            return

        i = 1
        while i < 10:
            i += 1
            time.sleep(10)
            progress = self._get_progress(
                pipeline_id=pipeline_id,
                update_id=update_id,
            )
            print(f"{self.name}, update_id: {update_id}, progress: {progress}")
            if progress:
                assert progress.lower() != "failed", f"Pipeline {self.name}: update failed"
                if progress.lower() == "canceled":
                    break
                if progress.lower() in states:
                    break


    def _compose_settings(
        self,
        continuous_workflow,
        ):

        settings = {
            "name": self.name,
            "edition": "Advanced",
            "development": True,
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
            "serverless": self.serverless,
            "configuration": {
                "pipelines.enableTrackHistory": "true"
            },
            "continuous": continuous_workflow
        }

        settings["configuration"].update(self.tags)

        return settings


    def _update(
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

        print(response.json())

        response.raise_for_status()

        if self.continuous_workflow:
            self._wait_until_state(pipeline_id=pipeline_id, states=["running"])


    def _create(
        self,
        workflow_settings,
        ):

        response = requests.post(
            url=f"https://{self.databricks_host}/api/2.0/pipelines",
            json=workflow_settings,
            headers={"Authorization": f"Bearer {self.databricks_token}"},
            timeout=60
            )

        print(response.json())

        response.raise_for_status()

        if self.continuous_workflow:
            self._wait_until_state(pipeline_id=self._get_id(), states=["running"])


    def _refresh(
        self,
        pipeline_id,
        full_refresh = False,
        refresh_selection = None,
        full_refresh_selection = None,
    ):
        if not refresh_selection:
            refresh_selection = []

        if not full_refresh_selection:
            full_refresh_selection = []

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

        print(response.json())

        response.raise_for_status()

        if self.continuous_workflow:
            states = ["running"]
        else:
            states = ["completed"]
        self._wait_until_state(pipeline_id=pipeline_id, states=states)


    def _get_progress(
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


    def _stop(self, pipeline_id):
        response = requests.post(
            url=f"https://{self.databricks_host}/api/2.0/pipelines/{pipeline_id}/stop",
            headers={"Authorization": f"Bearer {self.databricks_token}"},
            timeout=60,
        )

        response.raise_for_status()

        self._wait_until_state(pipeline_id=pipeline_id, states=[])
