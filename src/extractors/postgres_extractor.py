"""
Extract tables from PostgreSQL via JDBC into Spark DataFrames.

Supports full-load and incremental-load (based on a high-water-mark column).
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession

from src.utils.logger import get_logger
from src.utils.retry import retry

logger = get_logger(__name__)


class PostgresExtractor:
    """
    Read one or more PostgreSQL tables into Spark DataFrames.

    Parameters
    ----------
    spark : active SparkSession (must have the postgresql JDBC driver on
            the classpath — see ``spark.jars.packages`` in config).
    cfg   : the full pipeline config dict. Uses ``cfg["postgres"]``.
    """

    def __init__(self, spark: SparkSession, cfg: dict[str, Any]) -> None:
        self.spark = spark
        self.pg = cfg["postgres"]
        self._jdbc_url = (
            f"jdbc:postgresql://{self.pg['host']}:{self.pg['port']}"
            f"/{self.pg['database']}"
        )
        self._conn_props = {
            "user": str(self.pg["user"]),
            "password": str(self.pg["password"]),
            "driver": "org.postgresql.Driver",
            "fetchsize": "10000",
        }

    # ------------------------------------------------------------------
    @retry(max_attempts=3, backoff_seconds=5)
    def extract_table(
        self,
        table_cfg: dict[str, Any],
        high_water_mark: str | None = None,
    ) -> DataFrame:
        """
        Read a single table according to its config block.

        Parameters
        ----------
        table_cfg        : one element from ``cfg["postgres"]["tables"]``.
        high_water_mark  : if the table has an ``incremental_column``, only
                           rows **after** this value are fetched.
        """
        schema = self.pg.get("schema", "public")
        table_name = f"{schema}.{table_cfg['name']}"
        logger.info("Extracting %s from PostgreSQL …", table_name)

        reader = self.spark.read.format("jdbc").options(
            url=self._jdbc_url,
            dbtable=table_name,
            **self._conn_props,
        )

        # Partitioned read for parallelism
        part_col = table_cfg.get("partition_column")
        num_parts = table_cfg.get("num_partitions", 1)
        if part_col and num_parts > 1:
            bounds = self._get_bounds(table_name, part_col)
            if bounds:
                reader = reader.options(
                    partitionColumn=part_col,
                    lowerBound=str(bounds[0]),
                    upperBound=str(bounds[1]),
                    numPartitions=str(num_parts),
                )

        # Fetch-size override
        fetch = table_cfg.get("fetch_size")
        if fetch:
            reader = reader.option("fetchsize", str(fetch))

        df = reader.load()

        # Incremental filter
        inc_col = table_cfg.get("incremental_column")
        if inc_col and high_water_mark:
            df = df.filter(f"{inc_col} > '{high_water_mark}'")
            logger.info(
                "Incremental filter on %s > '%s'", inc_col, high_water_mark,
            )

        row_count = df.count()
        logger.info("Extracted %d rows from %s", row_count, table_name)
        return df

    # ------------------------------------------------------------------
    def extract_all(
        self,
        watermarks: dict[str, str] | None = None,
    ) -> dict[str, DataFrame]:
        """
        Extract every table listed in the config.

        Parameters
        ----------
        watermarks : optional mapping of ``table_name → high_water_mark``.

        Returns
        -------
        dict mapping table names to Spark DataFrames.
        """
        watermarks = watermarks or {}
        frames: dict[str, DataFrame] = {}
        for tbl in self.pg.get("tables", []):
            name = tbl["name"]
            hwm = watermarks.get(name)
            frames[name] = self.extract_table(tbl, high_water_mark=hwm)
        return frames

    # ------------------------------------------------------------------
    def _get_bounds(self, table: str, column: str) -> tuple[int, int] | None:
        """Query min/max of *column* for partitioned reads."""
        try:
            query = f"(SELECT MIN({column}) AS lo, MAX({column}) AS hi FROM {table}) AS bounds"
            row = (
                self.spark.read.format("jdbc")
                .options(url=self._jdbc_url, dbtable=query, **self._conn_props)
                .load()
                .first()
            )
            if row and row["lo"] is not None:
                return int(row["lo"]), int(row["hi"])
        except Exception as exc:
            logger.warning("Could not fetch bounds for %s.%s: %s", table, column, exc)
        return None
