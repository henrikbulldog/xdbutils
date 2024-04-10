from pyspark.sql.functions import col
from xdbutils.pipelines.management import DLTPipelineManager

class DLTPipelineDataManager(DLTPipelineManager):
    """ Delta Live Tables Pipeline Data Manager """

    def delete_persons(
        self,
        id_column,
        ids,
    ):
        pipeline_id = self._get_id()
        assert pipeline_id, f"Pipeline {self.source_system}-{self.entity} not found"

        update_id = self._get_latest_update(
            pipeline_id=pipeline_id,
        )
        assert update_id, f"Pipeline {self.source_system}-{self.entity}: latest update not found"

        save_continuous_workflow = self.continuous_workflow
        if self.continuous_workflow:
            print(f"Stopping pipeline {self.source_system}-{self.entity} and setting pipeline mode to triggered instead of continuous")
            self._stop(pipeline_id=pipeline_id)
            self.continuous_workflow = False
            self.create_or_update()

        datasets = self._get_datasets(
            pipeline_id=pipeline_id,
            update_id=update_id,
        )
        assert datasets, f"Pipeline {self.source_system}-{self.entity}: cannot find datasets"
        bronze_tables = [d for d in list(datasets) if d.startswith("bronze_")]
        non_bronze_tables = [
            d
            for d in list(datasets)
            if not d.startswith("bronze_") and not d.startswith("view_")
        ]

        for bronze_table in bronze_tables:
            ids_string = ",".join([f"'{id}'" for id in ids])
            statement = f"delete from {self.catalog}.{self.source_system}.{bronze_table} where `{id_column}` in ({ids_string})"
            print(statement)
            self.spark.sql(statement)
        
        print(f"Running pipeline {self.source_system}-{self.entity} with full refresh of: {', '.join(non_bronze_tables)}")
        self._refresh(
            pipeline_id=pipeline_id,
            full_refresh_selection=non_bronze_tables,
        )

        if self.continuous_workflow != save_continuous_workflow:
            print(f"Updating pipeline {self.source_system}-{self.entity}, setting pipeline mode back to continuous")
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


    def expose_tables(self, silver_catalog, gold_catalog):
        """ Expose DLT silver and gold tables from DLT catalog to other catalogs. """

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
                        print(f"Created view {silver_catalog}.{schema}.{table.replace("silver_", "")}")
                    if parts[0] == "gold":
                        self.spark.sql(f"create schema if not exists {gold_catalog}.{schema}")
                        self.spark.sql(f"""
                        create view if not exists {silver_catalog}.{schema}.{table.replace("gold_", "")}
                        as
                        select *
                        from {self.catalog}.{schema}.{table}
                        """)
                        print(f"Created view {silver_catalog}.{schema}.{table.replace("gold_", "")}")
