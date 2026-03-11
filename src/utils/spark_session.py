"""
Build a SparkSession from the pipeline configuration.

Handles both local dev and EMR/YARN deployments via the ``spark``
section in ``pipeline_config.yaml``.
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import SparkSession

from src.utils.logger import get_logger

logger = get_logger(__name__)


def build_spark_session(cfg: dict[str, Any]) -> SparkSession:
    """
    Create (or retrieve) a SparkSession configured from *cfg*.

    Parameters
    ----------
    cfg : the full pipeline config dict.  Uses ``cfg["spark"]``.
    """
    spark_cfg = cfg["spark"]

    builder = (
        SparkSession.builder
        .appName(spark_cfg.get("app_name", "etl_pipeline"))
        .master(spark_cfg.get("master", "local[*]"))
    )

    for key, value in spark_cfg.get("config", {}).items():
        builder = builder.config(key, str(value))

    # Snowflake connector staging area
    sf_cfg = cfg.get("snowflake", {})
    if sf_cfg.get("staging_area"):
        builder = builder.config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )

    spark = builder.getOrCreate()

    log_level = cfg.get("pipeline", {}).get("log_level", "WARN")
    spark.sparkContext.setLogLevel(log_level)

    logger.info(
        "SparkSession created — app=%s  master=%s",
        spark_cfg.get("app_name"),
        spark_cfg.get("master"),
    )
    return spark
