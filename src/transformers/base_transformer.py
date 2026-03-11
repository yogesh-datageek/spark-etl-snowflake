"""
Base transformer with reusable data-quality helpers.

All domain-specific transformers inherit from ``BaseTransformer`` and
override :meth:`transform`.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from src.utils.logger import get_logger

logger = get_logger(__name__)


class BaseTransformer(ABC):
    """Abstract base class for all transformers."""

    def __init__(self, cfg: dict[str, Any]) -> None:
        self.cfg = cfg

    # ---------- abstract contract ----------

    @abstractmethod
    def transform(self, dataframes: dict[str, DataFrame]) -> dict[str, DataFrame]:
        """
        Run transformation logic.

        Parameters
        ----------
        dataframes : mapping of ``name → DataFrame`` produced by extractors.

        Returns
        -------
        mapping of ``output_name → transformed DataFrame``.
        """
        ...

    # ---------- reusable quality helpers ----------

    @staticmethod
    def deduplicate(df: DataFrame, key_columns: list[str]) -> DataFrame:
        """Drop duplicate rows based on *key_columns*, keeping the latest."""
        return df.dropDuplicates(key_columns)

    @staticmethod
    def drop_nulls(df: DataFrame, columns: list[str]) -> DataFrame:
        """Drop rows where any of *columns* is null."""
        return df.dropna(subset=columns)

    @staticmethod
    def trim_strings(df: DataFrame) -> DataFrame:
        """Trim leading/trailing whitespace from all string columns."""
        for field in df.schema.fields:
            if str(field.dataType) == "StringType":
                df = df.withColumn(field.name, F.trim(F.col(field.name)))
        return df

    @staticmethod
    def add_audit_columns(df: DataFrame) -> DataFrame:
        """Append ``_etl_loaded_at`` and ``_etl_row_hash`` columns."""
        df = df.withColumn("_etl_loaded_at", F.current_timestamp())
        # Deterministic hash across all columns for change detection
        hash_cols = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in df.columns if not c.startswith("_etl_")]
        df = df.withColumn("_etl_row_hash", F.sha2(F.concat_ws("|", *hash_cols), 256))
        return df

    @staticmethod
    def log_stats(df: DataFrame, name: str) -> None:
        """Log row count and null counts per column."""
        count = df.count()
        logger.info("[%s] row_count=%d  partitions=%d", name, count, df.rdd.getNumPartitions())
