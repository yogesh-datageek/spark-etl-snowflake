"""
Extract datasets from S3 into Spark DataFrames.

Supports Parquet, CSV, and JSON with optional schema enforcement.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from src.utils.logger import get_logger
from src.utils.schema_helper import load_schema

logger = get_logger(__name__)


class S3Extractor:
    """
    Read datasets from S3 according to ``cfg["s3"]``.

    Parameters
    ----------
    spark : active SparkSession.
    cfg   : full pipeline config dict.
    """

    def __init__(self, spark: SparkSession, cfg: dict[str, Any]) -> None:
        self.spark = spark
        self.s3_cfg = cfg["s3"]
        self.bucket = self.s3_cfg["bucket"]
        self.region = self.s3_cfg.get("region", "us-east-1")

    # ------------------------------------------------------------------
    def extract_dataset(self, ds_cfg: dict[str, Any]) -> DataFrame:
        """
        Read one S3 dataset.

        Parameters
        ----------
        ds_cfg : one element from ``cfg["s3"]["datasets"]``.
        """
        name = ds_cfg["name"]
        path = f"s3a://{self.bucket}/{ds_cfg['prefix']}"
        fmt = ds_cfg.get("file_format", "parquet").lower()

        logger.info("Reading s3://%s/%s (%s) …", self.bucket, ds_cfg["prefix"], fmt)

        # Optional schema enforcement
        schema = None
        schema_path = ds_cfg.get("schema_path")
        if schema_path and Path(schema_path).exists():
            schema = load_schema(schema_path)
            logger.info("Applying schema from %s", schema_path)

        reader = self.spark.read.format(fmt)

        if schema:
            reader = reader.schema(schema)

        # Format-specific options
        if fmt == "csv":
            reader = reader.option("header", "true").option("inferSchema", "false")
        elif fmt == "json":
            reader = reader.option("multiLine", "true")

        df = reader.load(path)

        # Log partition stats
        partition_keys = ds_cfg.get("partition_keys", [])
        logger.info(
            "Loaded dataset '%s': %d partitions,  partition_keys=%s",
            name,
            df.rdd.getNumPartitions(),
            partition_keys or "none",
        )
        return df

    # ------------------------------------------------------------------
    def extract_all(self) -> dict[str, DataFrame]:
        """
        Extract every dataset listed in the S3 config block.

        Returns
        -------
        dict mapping dataset names to Spark DataFrames.
        """
        frames: dict[str, DataFrame] = {}
        for ds in self.s3_cfg.get("datasets", []):
            frames[ds["name"]] = self.extract_dataset(ds)
        return frames
