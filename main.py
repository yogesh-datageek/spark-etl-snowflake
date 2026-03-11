"""
ETL Pipeline — main entry-point.

Usage
-----
    spark-submit --deploy-mode cluster main.py [--env prod] [--config config/pipeline_config.yaml]

The pipeline executes three phases:
    1. **Extract**  — read from PostgreSQL and S3.
    2. **Transform** — apply PySpark business logic on EMR.
    3. **Load**      — write results to Snowflake.
"""

from __future__ import annotations

import argparse
import sys
import time
from typing import Any

from pyspark.sql import DataFrame

from src.utils.config_loader import load_config
from src.utils.logger import get_logger
from src.utils.spark_session import build_spark_session

from src.extractors.postgres_extractor import PostgresExtractor
from src.extractors.s3_extractor import S3Extractor

from src.transformers.business_transforms import (
    Customer360Transformer,
    InventorySnapshotTransformer,
    MaterialConsumptionTransformer,
    OrderEnrichmentTransformer,
    SupplierPerformanceTransformer,
)

from src.loaders.snowflake_loader import SnowflakeLoader


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ETL Pipeline — PG + S3 → EMR → Snowflake")
    parser.add_argument(
        "--config",
        default="config/pipeline_config.yaml",
        help="Path to the base YAML config file.",
    )
    parser.add_argument(
        "--env",
        default=None,
        help="Target environment (dev / staging / prod). "
             "Overrides PIPELINE_ENV and the value in the config file.",
    )
    parser.add_argument(
        "--steps",
        default="extract,transform,load",
        help="Comma-separated pipeline phases to run (default: all).",
    )
    return parser.parse_args()


# ======================================================================
#  Pipeline phases
# ======================================================================

def run_extract(
    cfg: dict[str, Any],
    spark,
    logger,
) -> dict[str, DataFrame]:
    """Phase 1 — Extract data from PostgreSQL and S3."""
    logger.info("=" * 60)
    logger.info("PHASE 1: EXTRACT")
    logger.info("=" * 60)

    all_frames: dict[str, DataFrame] = {}

    # --- PostgreSQL ---
    pg_extractor = PostgresExtractor(spark, cfg)
    pg_frames = pg_extractor.extract_all()
    all_frames.update(pg_frames)

    # --- S3 ---
    s3_extractor = S3Extractor(spark, cfg)
    s3_frames = s3_extractor.extract_all()
    all_frames.update(s3_frames)

    logger.info("Extraction complete — datasets: %s", list(all_frames.keys()))
    return all_frames


def run_transform(
    cfg: dict[str, Any],
    raw_frames: dict[str, DataFrame],
    logger,
) -> dict[str, DataFrame]:
    """Phase 2 — Apply business transformations."""
    logger.info("=" * 60)
    logger.info("PHASE 2: TRANSFORM")
    logger.info("=" * 60)

    transformed: dict[str, DataFrame] = {}

    # --- Customer 360 ---
    c360 = Customer360Transformer(cfg)
    transformed.update(c360.transform(raw_frames))

    # --- Order enrichment ---
    orders = OrderEnrichmentTransformer(cfg)
    transformed.update(orders.transform(raw_frames))

    # --- Inventory snapshot ---
    if "inventory" in raw_frames:
        inv = InventorySnapshotTransformer(cfg)
        transformed.update(inv.transform(raw_frames))

    # --- Material consumption ---
    if "raw_materials" in raw_frames and "purchase_orders" in raw_frames:
        mat = MaterialConsumptionTransformer(cfg)
        transformed.update(mat.transform(raw_frames))

    # --- Supplier performance ---
    if "suppliers" in raw_frames and "purchase_orders" in raw_frames:
        sup = SupplierPerformanceTransformer(cfg)
        transformed.update(sup.transform(raw_frames))

    # Optionally cache staging output to S3 for debugging
    staging_path = cfg.get("spark", {}).get("staging_path")
    if staging_path:
        for name, df in transformed.items():
            out = f"{staging_path}{name}"
            df.write.mode("overwrite").parquet(out)
            logger.info("Staged %s → %s", name, out)

    logger.info("Transformation complete — outputs: %s", list(transformed.keys()))
    return transformed


def run_load(
    cfg: dict[str, Any],
    spark,
    transformed: dict[str, DataFrame],
    logger,
) -> None:
    """Phase 3 — Load into Snowflake."""
    logger.info("=" * 60)
    logger.info("PHASE 3: LOAD")
    logger.info("=" * 60)

    loader = SnowflakeLoader(spark, cfg)
    loader.write_all(transformed)

    logger.info("Load complete.")


# ======================================================================
#  Main
# ======================================================================

def main() -> None:
    args = parse_args()
    cfg = load_config(base_path=args.config, env=args.env)

    log_level = cfg.get("pipeline", {}).get("log_level", "INFO")
    logger = get_logger("pipeline", level=log_level)

    logger.info(
        "Pipeline '%s' starting — env=%s",
        cfg["pipeline"]["name"],
        cfg["pipeline"]["environment"],
    )

    spark = build_spark_session(cfg)
    steps = {s.strip().lower() for s in args.steps.split(",")}
    start = time.time()

    try:
        raw_frames: dict[str, DataFrame] = {}
        transformed: dict[str, DataFrame] = {}

        if "extract" in steps:
            raw_frames = run_extract(cfg, spark, logger)

        if "transform" in steps:
            if not raw_frames:
                logger.error("No extracted data available — run 'extract' step first.")
                sys.exit(1)
            transformed = run_transform(cfg, raw_frames, logger)

        if "load" in steps:
            if not transformed:
                logger.error("No transformed data available — run 'transform' step first.")
                sys.exit(1)
            run_load(cfg, spark, transformed, logger)

        elapsed = time.time() - start
        logger.info("Pipeline finished successfully in %.1f s", elapsed)

    except Exception:
        logger.exception("Pipeline FAILED")
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
