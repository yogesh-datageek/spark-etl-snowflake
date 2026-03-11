"""
Load Spark DataFrames into Snowflake using the Spark-Snowflake connector.

Supports ``overwrite``, ``append``, and ``merge`` (via a staging-table
pattern) write modes.
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession

from src.utils.logger import get_logger
from src.utils.retry import retry

logger = get_logger(__name__)


class SnowflakeLoader:
    """
    Write DataFrames to Snowflake tables.

    Parameters
    ----------
    spark : active SparkSession.
    cfg   : full pipeline config dict.  Uses ``cfg["snowflake"]``.
    """

    SFIO_FORMAT = "net.snowflake.spark.snowflake"

    def __init__(self, spark: SparkSession, cfg: dict[str, Any]) -> None:
        self.spark = spark
        self.sf_cfg = cfg["snowflake"]
        self._sf_options = {
            "sfURL": f"{self.sf_cfg['account']}.snowflakecomputing.com",
            "sfUser": str(self.sf_cfg["user"]),
            "sfPassword": str(self.sf_cfg["password"]),
            "sfRole": str(self.sf_cfg.get("role", "")),
            "sfWarehouse": str(self.sf_cfg.get("warehouse", "")),
            "sfDatabase": str(self.sf_cfg.get("database", "")),
            "sfSchema": str(self.sf_cfg.get("schema", "PUBLIC")),
        }
        staging = self.sf_cfg.get("staging_area")
        if staging:
            self._sf_options["tempDir"] = staging

    # ------------------------------------------------------------------
    @retry(max_attempts=3, backoff_seconds=10)
    def write(
        self,
        df: DataFrame,
        target_table: str,
        mode: str = "overwrite",
        merge_keys: list[str] | None = None,
    ) -> None:
        """
        Write *df* to Snowflake.

        Parameters
        ----------
        df           : Spark DataFrame to write.
        target_table : destination Snowflake table name.
        mode         : ``overwrite`` | ``append`` | ``merge``.
        merge_keys   : columns used for the MERGE join (required when
                       ``mode="merge"``).
        """
        if mode == "merge" and merge_keys:
            self._merge_write(df, target_table, merge_keys)
        else:
            self._simple_write(df, target_table, mode)

    # ------------------------------------------------------------------
    def _simple_write(self, df: DataFrame, table: str, mode: str) -> None:
        """Overwrite or append write."""
        logger.info(
            "Writing %s to Snowflake table %s (mode=%s) …", df, table, mode,
        )
        (
            df.write
            .format(self.SFIO_FORMAT)
            .options(**self._sf_options)
            .option("dbtable", table)
            .mode(mode)
            .save()
        )
        logger.info("✓ Wrote to %s successfully.", table)

    # ------------------------------------------------------------------
    def _merge_write(
        self,
        df: DataFrame,
        target_table: str,
        merge_keys: list[str],
    ) -> None:
        """
        Upsert via a staging table + Snowflake MERGE statement.

        1. Write incoming data to a temporary staging table.
        2. Execute a MERGE INTO … USING … ON (keys) WHEN MATCHED … WHEN NOT MATCHED.
        3. Drop the staging table.
        """
        staging_table = f"{target_table}__staging"

        logger.info("Merge-write to %s via staging table %s …", target_table, staging_table)

        # Step 1 — write to staging
        self._simple_write(df, staging_table, mode="overwrite")

        # Step 2 — build & run MERGE SQL
        all_cols = df.columns
        on_clause = " AND ".join(
            f"target.{k} = staging.{k}" for k in merge_keys
        )
        update_set = ", ".join(
            f"target.{c} = staging.{c}" for c in all_cols if c not in merge_keys
        )
        insert_cols = ", ".join(all_cols)
        insert_vals = ", ".join(f"staging.{c}" for c in all_cols)

        db = self._sf_options.get("sfDatabase", "")
        schema = self._sf_options.get("sfSchema", "PUBLIC")
        fq_target = f"{db}.{schema}.{target_table}"
        fq_staging = f"{db}.{schema}.{staging_table}"

        merge_sql = f"""
            MERGE INTO {fq_target} AS target
            USING {fq_staging} AS staging
            ON {on_clause}
            WHEN MATCHED THEN UPDATE SET {update_set}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """

        self._run_snowflake_sql(merge_sql)

        # Step 3 — drop staging
        self._run_snowflake_sql(f"DROP TABLE IF EXISTS {fq_staging}")
        logger.info("✓ Merge into %s complete.", target_table)

    # ------------------------------------------------------------------
    def _run_snowflake_sql(self, sql: str) -> None:
        """Execute arbitrary SQL via the Snowflake connector's ``utils.runQuery``."""
        from py4j.java_gateway import java_import  # type: ignore[import-untyped]

        java_import(
            self.spark._jvm,  # type: ignore[union-attr]
            "net.snowflake.spark.snowflake.Utils",
        )
        self.spark._jvm.net.snowflake.spark.snowflake.Utils.runQuery(  # type: ignore[union-attr]
            self._sf_options, sql,
        )
        logger.info("Executed SQL on Snowflake: %s", sql[:120])

    # ------------------------------------------------------------------
    def write_all(self, dataframes: dict[str, DataFrame]) -> None:
        """
        Write every DataFrame using the table mappings in the config.

        Falls back to the global ``write_mode`` when a table-specific
        mapping is not found.
        """
        table_map = {
            t["source"]: t
            for t in self.sf_cfg.get("tables", [])
        }
        default_mode = self.sf_cfg.get("write_mode", "overwrite")

        for name, df in dataframes.items():
            mapping = table_map.get(name, {})
            target = mapping.get("target", name.upper())
            mode = mapping.get("write_mode", default_mode)
            merge_keys = mapping.get("merge_keys")
            self.write(df, target, mode=mode, merge_keys=merge_keys)
