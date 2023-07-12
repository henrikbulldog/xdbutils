""" Delta Live Tables Pipelines """

from typing import Callable
from pyspark.sql import DataFrame
import dlt  # pylint: disable=import-error


class Pipeline():
    """ Delta Live Tables Pipeline """

    def __init__(self, source_system, entity, raw_base_path):
        self._source_system = source_system
        self._entity = entity
        self._raw_base_path = raw_base_path

    def raw_to_bronze(self,
                      load: Callable[[str], DataFrame],
                      transform: Callable[[DataFrame], DataFrame] = None):
        """ Raw to bronze """

        class_path = f"{self._source_system}/{self._entity}"
        source_path = f"{self._raw_base_path}/{class_path}"

        @dlt.table(
            comment=f"Raw to Bronze, {self._source_system}.{self._entity}",
            name=f"bronze_{self._entity}"
        )
        def raw_to_bronze_table():
            df = load(source_path)
            if transform:
                df = transform(df)
            return df

    def bronze_to_silver(
        self,
        transform: Callable[[DataFrame], DataFrame] = None,
        expectations={}):
        """ Bronze to Silver """

        @dlt.table(
            comment=f"Bronze to Silver, {self._source_system}.{self._entity}",
            name=f"silver_{self._entity}"
        )
        @dlt.expect_all(expectations)
        def bronze_to_silver_table():
            df = dlt.read(f"bronze_{self._entity}")
            if transform:
                df = transform(df)
            return df
