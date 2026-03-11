# ETL Pipeline — PostgreSQL + S3 → EMR/PySpark → Snowflake

A production-ready, config-driven ETL pipeline that **extracts** data from PostgreSQL and Amazon S3, **transforms** it with PySpark on an EMR cluster, and **loads** the results into Snowflake.

---

## Architecture

```
┌────────────┐      ┌────────────────────────────────┐      ┌────────────┐
│ PostgreSQL │─────▶│                                │─────▶│            │
│  (JDBC)    │      │    Amazon EMR — PySpark         │      │ Snowflake  │
└────────────┘      │                                │      │            │
                    │  • Customer 360                 │      └────────────┘
┌────────────┐      │  • Order Enrichment             │
│  Amazon S3 │─────▶│  • Inventory Snapshot           │
│  (Parquet/ │      │  • Material Consumption         │
│   JSON)    │      │  • Supplier Performance         │
└────────────┘      └────────────────────────────────┘
```

### Data Flow

| Phase       | Source(s)                      | Output                      |
|-------------|-------------------------------|-----------------------------|
| Extract     | PostgreSQL tables, S3 datasets | Raw Spark DataFrames        |
| Transform   | Raw DataFrames                | `customer_360`, `order_enriched`, `inventory_snapshot`, `material_consumption`, `supplier_performance` |
| Load        | Transformed DataFrames        | Snowflake tables (`DIM_CUSTOMER_360`, `FACT_ORDERS_ENRICHED`, `FACT_INVENTORY_SNAPSHOT`, `FACT_MATERIAL_CONSUMPTION`, `DIM_SUPPLIER_PERFORMANCE`) |

---

## Project Structure

```
etl-pipeline/
├── config/
│   ├── pipeline_config.yaml      # Main config (sources, transforms, targets)
│   ├── env/
│   │   ├── dev.yaml              # Dev overrides
│   │   └── prod.yaml             # Prod overrides
│   └── schemas/
│       ├── inventory.json        # Spark schema for S3 inventory snapshots
│       ├── raw_materials.json    # Spark schema for S3 raw materials
│       └── product_reviews.json  # Spark schema for S3 reviews data
├── src/
│   ├── extractors/
│   │   ├── postgres_extractor.py # JDBC reader with partitioned + incremental support
│   │   └── s3_extractor.py       # Parquet / CSV / JSON reader with schema enforcement
│   ├── transformers/
│   │   ├── base_transformer.py   # Shared quality helpers (dedup, trim, audit cols)
│   │   └── business_transforms.py# Customer360, OrderEnrichment, InventorySnapshot,
│   │                              # MaterialConsumption, SupplierPerformance
│   ├── loaders/
│   │   └── snowflake_loader.py   # Spark-Snowflake connector (overwrite/append/merge)
│   └── utils/
│       ├── config_loader.py      # YAML loader with env-var interpolation + deep merge
│       ├── logger.py             # JSON-structured logging
│       ├── retry.py              # Retry decorator with exponential backoff
│       ├── schema_helper.py      # JSON → PySpark StructType converter
│       └── spark_session.py      # SparkSession builder from config
├── scripts/
│   └── submit_emr_job.sh         # Create EMR cluster + submit Spark step
├── tests/
│   └── test_utils.py             # Unit tests for config loader, schema, retry, logger
├── main.py                       # Pipeline entry-point
├── requirements.txt
├── pyproject.toml
├── .env.example
├── .gitignore
└── README.md
```

---

## Transformations

### Customer 360
Joins **customers** + **orders** + **product_reviews** into a unified customer profile with lifetime value, average order value, review ratings, tenure, and customer segmentation (platinum / gold / silver / bronze).

### Order Enrichment
Enriches **orders** with **product** details, adds per-customer order ranking (most recent first), and time-based features (year, month, day-of-week).

### Inventory Snapshot
Joins **inventory** snapshots (S3) with **products** and **warehouses** (PostgreSQL). Computes stock valuation, reorder flags, days since last restock, stock health categories (out_of_stock / low_stock / healthy / overstock), and per-warehouse aggregates (total value, SKU count).

### Material Consumption
Links **purchase_orders** to **raw_materials** and **suppliers**. Computes line-item costs, delivery variance (actual vs expected lead time), late-delivery flags, rolling 90-day material spend, and time features.

### Supplier Performance
Aggregates purchase order data into a **per-supplier scorecard** with KPIs: total spend, on-time delivery rate, late deliveries, short shipments, quantity accuracy, and a tiered classification (strategic / preferred / approved / under_review).

---

## Prerequisites

- **Python** ≥ 3.10
- **Apache Spark** ≥ 3.4 (or an EMR 7.x cluster)
- **AWS CLI** configured with permissions for S3 and EMR
- **PostgreSQL** instance accessible from the Spark cluster
- **Snowflake** account with a warehouse and role for ETL

---

## Quick Start

### 1. Clone and install

```bash
git clone https://github.com/<your-org>/pyspark-etl-pipeline.git
cd pyspark-etl-pipeline
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure

```bash
cp .env.example .env
# Edit .env with your real credentials
```

All connection strings, table lists, and Spark tuning knobs live in `config/pipeline_config.yaml`. Environment-specific overrides (warehouse size, shuffle partitions, etc.) go in `config/env/<env>.yaml`.

The config supports `${ENV_VAR:-default}` syntax, so sensitive values can be injected at runtime via environment variables or a secrets manager.

### 3. Run locally (dev)

```bash
export PIPELINE_ENV=dev
source .env

spark-submit \
  --packages net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.4,net.snowflake:snowflake-jdbc:3.16.1,org.postgresql:postgresql:42.7.3 \
  main.py --env dev
```

Run only specific phases:

```bash
spark-submit main.py --env dev --steps extract,transform
```

### 4. Run on EMR (prod)

```bash
export PIPELINE_ENV=prod
source .env

chmod +x scripts/submit_emr_job.sh
./scripts/submit_emr_job.sh --env prod
```

The script uploads code to S3, creates an auto-terminating EMR cluster, and submits the pipeline as a Spark step. To reuse an existing cluster:

```bash
./scripts/submit_emr_job.sh --env prod --cluster-id j-XXXXXXXXXXXXX
```

---

## Configuration Reference

### `config/pipeline_config.yaml`

| Section      | Key Fields | Description |
|-------------|-----------|-------------|
| `pipeline`  | `name`, `environment`, `log_level`, `retry_*` | Global settings |
| `postgres`  | `host`, `port`, `database`, `tables[]` | JDBC source; each table supports `incremental_column`, `partition_column`, `num_partitions` |
| `s3`        | `bucket`, `datasets[]` | Each dataset specifies `prefix`, `file_format`, optional `schema_path` |
| `spark`     | `master`, `deploy_mode`, `config{}` | Spark tuning; `config` map is applied as `spark.conf.set()` calls |
| `snowflake` | `account`, `user`, `warehouse`, `tables[]` | Each table specifies `write_mode` (`overwrite` / `append` / `merge`) and optional `merge_keys` |
| `emr`       | `release_label`, `instance_type`, `core_instance_count` | Used by the submit script |

### Environment overrides

Files in `config/env/` are **deep-merged** on top of the base config. Only the keys you want to change need to be present.

### Environment variables

Every `${VAR:-default}` placeholder in the YAML is resolved at load time. See `.env.example` for the full list of supported variables.

---

## Adding a New Source or Transform

### New PostgreSQL table

Add an entry to `postgres.tables` in the config:

```yaml
- name: "inventory_adjustments"
  primary_key: "adjustment_id"
  incremental_column: "adjusted_at"
  partition_column: "adjustment_id"
  num_partitions: 8
  fetch_size: 5000
```

The extractor will pick it up automatically.

### New S3 dataset

Add an entry to `s3.datasets` and create a matching schema JSON:

```yaml
- name: "shipping_logs"
  prefix: "raw/shipping/"
  file_format: "parquet"
  schema_path: "config/schemas/shipping_logs.json"
  partition_keys: ["year", "month"]
```

### New transformation

1. Create a class in `src/transformers/` that extends `BaseTransformer`.
2. Implement the `transform(self, dfs) -> dict[str, DataFrame]` method.
3. Wire it into `main.py :: run_transform()`.
4. Add a Snowflake target mapping under `snowflake.tables`.

---

## Tests

```bash
pytest tests/ -v
```

The test suite covers the config loader (interpolation, deep merge, defaults), schema helper, retry decorator, and structured logger. Spark-dependent integration tests require a local Spark installation.

---

## Snowflake Write Modes

| Mode       | Behaviour |
|-----------|-----------|
| `overwrite` | Truncates and replaces the target table. |
| `append`    | Inserts new rows without touching existing data. |
| `merge`     | Upserts via a staging table + `MERGE INTO` on the configured `merge_keys`. |

---

## License

MIT
