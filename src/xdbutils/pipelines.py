""" Delta Live Tables Pipelines """

from typing import Callable
from pyspark.sql import DataFrame
import dlt  # pylint: disable=import-error


class Pipeline():
    """ Delta Live Tables Pipeline """

    def __init__(self, raw_base_path):
        self._raw_base_path = raw_base_path

    def raw_to_bronze(self,
        source_system,
        entity,
        load: Callable[[str], DataFrame],
        transform: Callable[[DataFrame], DataFrame] = None):
        """ Raw to bronze """

        class_path = f"{source_system}/{entity}"
        source_path = f"{self._raw_base_path}/{class_path}"

        @dlt.table(
            comment=f"Raw to Bronze, {source_system}.{entity}",
            name=f"bronze_{entity}"
        )
        def raw_to_bronze_table():
            df = load(source_path)
            if transform:
                df = transform(df)
            return df

    def bronze_to_silver(
        self,
        source_system,
        entity,
        transform: Callable[[DataFrame], DataFrame] = None,
        expectations={}):
        """ Bronze to Silver """

        @dlt.table(
            comment=f"Bronze to Silver, {source_system}.{entity}",
            name=f"silver_{entity}"
        )
        @dlt.expect_all(expectations)
        def bronze_to_silver_table():
            df = dlt.read(f"bronze_{entity}")
            if transform:
                df = transform(df)
            return df
