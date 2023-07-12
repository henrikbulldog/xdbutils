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
            comment=f"Raw to bronze, {self._source_system}.{self._entity}",
            name=f"bronze_{self._entity}"
        )
        def raw_to_bronze_table():
            raw_df = load(source_path)
            if transform:
                raw_df = transform(raw_df)
            return raw_df
