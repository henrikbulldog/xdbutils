"""Delta Live Tables data manager"""

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from xdbutils.pipelines.management import DLTPipelineManager

class DLTPipelineDataManager(DLTPipelineManager):
    """Delta Live Tables data manager"""

    def __init__(
        self,
        spark: SparkSession,
        dbutils,
        catalog: str,
        source_system: str,
        source_class: str,
        raw_base_path: str,
        name: str = None,
        tags: dict = None,
        continuous_workflow: bool = False,
        serverless: bool = False,
        databricks_token: str = None,
        databricks_host: str = None,
        source_path: str = None,
        ):

        super().__init__(
            spark=spark,
            dbutils=dbutils,
            source_system=source_system,
            source_class=source_class,
            catalog=catalog,
            name=name,
            tags=tags,
            continuous_workflow=continuous_workflow,
            serverless=serverless,
            databricks_token=databricks_token,
            databricks_host=databricks_host,
            source_path=source_path,
        )
        self.raw_base_path = raw_base_path


    def help(self):
        """Help"""
        print("Delta Live Tables data manager")
        print("delete_persons() -> Delete personally identifiable data from a Delta Live Table pipeline")
        print("expose_tables() -> Expose silver and gold tables as views in another catalog")
        print("backup_bronze() -> Expose silver and gold tables as views in another catalog")
        print("ingest_historical() -> Copy historical data from external location to raw hsitorical folder")


    def delete_persons(
        self,
        id_column: str,
        ids: list[str],
    ):
        """Delete personally identifiable data from a Delta Live Table pipeline"""

        pipeline_id = self._get_id()
        assert pipeline_id, f"Pipeline {self.name} not found"

        update_id = self._get_latest_update(
            pipeline_id=pipeline_id,
        )
        assert update_id, f"Pipeline {self.name}: latest update not found"

        save_continuous_workflow = self.continuous_workflow
        if self.continuous_workflow:
            self._stop(pipeline_id=pipeline_id)
            print(f"Stopping pipeline {self.name} and setting pipeline mode to triggered instead of continuous")
            self.continuous_workflow = False
            self.create_or_update()

        datasets = self._get_datasets(
            pipeline_id=pipeline_id,
            update_id=update_id,
        )
        assert datasets, f"Pipeline {self.name}: cannot find datasets"

        ids_string = ",".join([f"'{id}'" for id in ids])
        refresh_tables = []
        bronze_updated = False
        silver_updated = False

        bronze_tables = [d for d in list(datasets) if d.startswith("bronze_")]
        for bronze_table in bronze_tables:
            df = self.spark.sql(f"select * from {self.catalog}.{self.source_system}.{bronze_table}")
            if any(column == id_column for column in df.columns):
                bronze_updated = True
                statement = f"delete from {self.catalog}.{self.source_system}.{bronze_table} where `{id_column}` in ({ids_string})"
                print(statement)
                self.spark.sql(statement)

        if bronze_updated:
            refresh_tables = [d for d in list(datasets) if not d.startswith("bronze_") and not d.startswith("view_")]
        else:
            silver_tables = [d for d in list(datasets) if d.startswith("silver_") and not d.startswith("view_")]
            for silver_table in silver_tables:
                df = self.spark.sql(f"select * from {self.catalog}.{self.source_system}.{silver_table}")
                if any(column == id_column for column in df.columns):
                    silver_updated = True
                    statement = f"delete from {self.catalog}.{self.source_system}.{silver_table} where `{id_column}` in ({ids_string})"
                    print(statement)
                    self.spark.sql(statement)
        
        if silver_updated:
            refresh_tables = [d for d in list(datasets) if not d.startswith("bronze_") and not d.startswith("silver_") and not d.startswith("view_")]

        if not bronze_updated and not silver_updated:
            print(f"Column {id_column} not found in bronze or silver tables.")

        if len(refresh_tables) > 0:
            print(f"Running pipeline {self.name} with full refresh of: {', '.join(refresh_tables)}")
            self._refresh(
                pipeline_id=pipeline_id,
                full_refresh_selection=refresh_tables,
            )

        if self.continuous_workflow != save_continuous_workflow:
            print(f"Updating pipeline {self.name}, setting pipeline mode back to continuous")
            self.continuous_workflow = save_continuous_workflow
            self.create_or_update()

        tables = [d for d in list(datasets) if not d.startswith("view_")]
        for table in tables:
            df = self.spark.sql(f"select * from {self.catalog}.{self.source_system}.{table}")
            if any(column == id_column for column in df.columns):
                print(f"Checking table {table} for {id_column} in {ids}")
                assert (
                    df.where(col(id_column).isin(ids)).count() == 0
                ), f"{table} contains {id_column} in {ids}"


    def expose_tables(
        self,
        silver_catalog: str,
        gold_catalog: str,
        ):
        """Expose silver and gold tables as views in another catalog"""

        schemas = [r[0] for r in (
                   self.spark.sql(f"show schemas in {self.catalog}")
                   .select("databaseName"))
                   .collect()]

        for schema in schemas:
            tables = [r[0] for r in (
                self.spark.sql(f"show tables in {self.catalog}.{schema}")
                .select("tableName")
                ).collect()]
            for table in tables:
                parts = table.split("_")
                if len(parts) > 1:
                    if parts[0] == "silver":
                        self.spark.sql(f"create schema if not exists {silver_catalog}.{schema}")
                        self.spark.sql(f"""
                        create view if not exists {silver_catalog}.{schema}.{table.replace("silver_", "")}
                        as
                        select *
                        from {self.catalog}.{schema}.{table}
                        """)
                    if parts[0] == "gold":
                        self.spark.sql(f"create schema if not exists {gold_catalog}.{schema}")
                        self.spark.sql(f"""
                        create view if not exists {silver_catalog}.{schema}.{table.replace("gold_", "")}
                        as
                        select *
                        from {self.catalog}.{schema}.{table}
                        """)


    def backup_bronze(self, source_class: str = None):
        """Expose silver and gold tables as views in another catalog"""

        if not source_class:
            source_class = self.source_class

        input_table = f"{self.catalog}.{self.source_system}.bronze_{source_class}"
        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        backup_path = f"{self.raw_base_path}/backup/{ts}/{self.source_system}/{source_class}"
        print(f"Writing {input_table} to {backup_path}")
        bronze_df = self.spark.sql(f"select * from {input_table}")
        (
            bronze_df
            .drop("_rescued_data")
            .drop("_ingest_time")
            .drop("_quarantined")
            .write
            .mode("overwrite")
            .parquet(backup_path)
        )
        return backup_path


    def ingest_historical(
        self,
        input_location: str,
        input_format: str = "parquet",
        source_class: str = None,
        ):
        """Copy historical data from external location to raw hsitorical folder"""

        if not source_class:
            source_class = f"{self.source_class}_historical"

        target_location = f"{self.raw_base_path}/{self.source_system}/{source_class}"
        print(f"Adding {input_location} to {target_location}")
        input_df = self.spark.read.format(input_format).load(input_location)
        (
            input_df
            .write
            .mode("append")
            .parquet(target_location)
        )
