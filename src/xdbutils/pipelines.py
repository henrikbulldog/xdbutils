""" Delta Live Tables Pipelines """

from typing import Callable
from pyspark.sql import DataFrame
import dlt  # pylint: disable=import-error


class Pipeline():
    """ Delta Live Tables Pipeline """

    def raw_to_bronze(
        self,
        source_system,
        entity,
        raw_df: DataFrame
        ):
        """ Raw to bronze """

        @dlt.table(
            comment=f"Raw to Bronze, {source_system}.{entity}",
            name=f"bronze_{entity}"
        )
        def raw_to_bronze_table():
            return raw_df

    def bronze_to_silver(
        self,
        source_system,
        entity,
        transform: Callable[[DataFrame], DataFrame] = None,
        expectations={}
        ):
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

    def silver_to_gold(
        self,
        name,
        source_system,
        entity,
        transform: Callable[[DataFrame], DataFrame] = None,
        expectations={}):
        """ Bronze to Silver """

        @dlt.table(
            comment=f"Silver to Gold, {source_system}.{entity}_{name}",
            name=f"gold_{entity}_{name}"
        )
        @dlt.expect_all(expectations)
        def silver_table():
            df = dlt.read(f"silver_{entity}")
            if transform:
                df = transform(df)
            return df
