"""
Domain-specific PySpark transformations.

Each class produces one output DataFrame that will be loaded into Snowflake.
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

from src.transformers.base_transformer import BaseTransformer
from src.utils.logger import get_logger

logger = get_logger(__name__)


# ======================================================================
#  Customer 360 — joins customers + orders + reviews
# ======================================================================
class Customer360Transformer(BaseTransformer):
    """
    Build a customer-360 view by joining:
      • customers  (PostgreSQL)
      • orders     (PostgreSQL)
      • product_reviews (S3)
    """

    def transform(self, dfs: dict[str, DataFrame]) -> dict[str, DataFrame]:
        customers = dfs["customers"]
        orders = dfs["orders"]
        reviews = dfs.get("product_reviews")

        # ---- clean ----
        customers = self.trim_strings(customers)
        customers = self.deduplicate(customers, ["customer_id"])

        # ---- aggregate order metrics per customer ----
        order_agg = (
            orders
            .groupBy("customer_id")
            .agg(
                F.count("order_id").alias("total_orders"),
                F.sum("order_amount").alias("lifetime_value"),
                F.avg("order_amount").alias("avg_order_value"),
                F.max("order_date").alias("last_order_date"),
                F.min("order_date").alias("first_order_date"),
            )
        )

        # ---- join customers <> orders ----
        c360 = customers.join(order_agg, on="customer_id", how="left")

        # ---- optionally join average review rating per customer ----
        if reviews is not None:
            review_agg = (
                reviews
                .groupBy("user_id")
                .agg(
                    F.avg("rating").alias("avg_review_rating"),
                    F.count("review_id").alias("total_reviews"),
                )
            )
            c360 = c360.join(
                review_agg,
                c360["customer_id"] == review_agg["user_id"],
                how="left",
            ).drop("user_id")

        # ---- derived columns ----
        c360 = c360.withColumn(
            "customer_tenure_days",
            F.datediff(F.current_date(), F.col("first_order_date")),
        ).withColumn(
            "days_since_last_order",
            F.datediff(F.current_date(), F.col("last_order_date")),
        ).withColumn(
            "customer_segment",
            F.when(F.col("lifetime_value") >= 10000, "platinum")
            .when(F.col("lifetime_value") >= 5000, "gold")
            .when(F.col("lifetime_value") >= 1000, "silver")
            .otherwise("bronze"),
        )

        c360 = self.add_audit_columns(c360)
        self.log_stats(c360, "customer_360")
        return {"customer_360": c360}


# ======================================================================
#  Order Enrichment — adds product details + ranking
# ======================================================================
class OrderEnrichmentTransformer(BaseTransformer):
    """Enrich orders with product info and per-customer order ranking."""

    def transform(self, dfs: dict[str, DataFrame]) -> dict[str, DataFrame]:
        orders = dfs["orders"]
        products = dfs["products"]

        orders = self.trim_strings(orders)
        orders = self.deduplicate(orders, ["order_id"])

        # ---- join with products ----
        enriched = orders.join(products, on="product_id", how="left")

        # ---- per-customer order rank ----
        window = Window.partitionBy("customer_id").orderBy(F.col("order_date").desc())
        enriched = enriched.withColumn("order_rank", F.row_number().over(window))

        # ---- time features ----
        enriched = (
            enriched
            .withColumn("order_year", F.year("order_date"))
            .withColumn("order_month", F.month("order_date"))
            .withColumn("order_dow", F.dayofweek("order_date"))
        )

        enriched = self.add_audit_columns(enriched)
        self.log_stats(enriched, "order_enriched")
        return {"order_enriched": enriched}


# ======================================================================
#  Inventory Snapshot — enriched stock-level view per warehouse
# ======================================================================
class InventorySnapshotTransformer(BaseTransformer):
    """
    Build an enriched inventory snapshot by joining:
      • inventory    (S3 — daily/periodic snapshots)
      • products     (PostgreSQL)
      • warehouses   (PostgreSQL)

    Adds stock valuation, reorder flags, and days-of-supply estimates.
    """

    def transform(self, dfs: dict[str, DataFrame]) -> dict[str, DataFrame]:
        inventory = dfs["inventory"]
        products = dfs["products"]
        warehouses = dfs["warehouses"]

        inventory = self.deduplicate(inventory, ["inventory_id"])

        # ---- join with product and warehouse dimensions ----
        enriched = (
            inventory
            .join(products, on="product_id", how="left")
            .join(warehouses, on="warehouse_id", how="left")
        )

        # ---- stock valuation ----
        enriched = enriched.withColumn(
            "stock_value",
            F.col("quantity_on_hand") * F.col("unit_cost"),
        )

        # ---- reorder flag: stock at or below reorder level ----
        enriched = enriched.withColumn(
            "needs_reorder",
            F.when(F.col("quantity_on_hand") <= F.col("reorder_level"), True)
            .otherwise(False),
        )

        # ---- days since last restock ----
        enriched = enriched.withColumn(
            "days_since_restock",
            F.datediff(F.current_date(), F.col("last_restock_date")),
        )

        # ---- stock health category ----
        enriched = enriched.withColumn(
            "stock_health",
            F.when(F.col("quantity_on_hand") == 0, "out_of_stock")
            .when(F.col("needs_reorder"), "low_stock")
            .when(
                F.col("quantity_on_hand") > F.col("reorder_level") * 5,
                "overstock",
            )
            .otherwise("healthy"),
        )

        # ---- aggregate metrics per warehouse ----
        enriched = enriched.withColumn(
            "warehouse_total_value",
            F.sum("stock_value").over(Window.partitionBy("warehouse_id")),
        ).withColumn(
            "warehouse_sku_count",
            F.count("product_id").over(Window.partitionBy("warehouse_id")),
        )

        enriched = self.add_audit_columns(enriched)
        self.log_stats(enriched, "inventory_snapshot")
        return {"inventory_snapshot": enriched}


# ======================================================================
#  Material Consumption — links purchase orders to raw materials
# ======================================================================
class MaterialConsumptionTransformer(BaseTransformer):
    """
    Build a material consumption fact table by joining:
      • purchase_orders  (PostgreSQL)
      • raw_materials    (S3)
      • suppliers        (PostgreSQL)

    Computes cost breakdowns, lead-time analysis, and consumption trends.
    """

    def transform(self, dfs: dict[str, DataFrame]) -> dict[str, DataFrame]:
        po = dfs["purchase_orders"]
        materials = dfs["raw_materials"]
        suppliers = dfs["suppliers"]

        po = self.trim_strings(po)
        po = self.deduplicate(po, ["po_id"])

        # ---- join PO -> materials -> suppliers ----
        consumption = (
            po
            .join(materials, on="material_id", how="left")
            .join(suppliers, on="supplier_id", how="left")
        )

        # ---- line-item cost ----
        consumption = consumption.withColumn(
            "line_cost",
            F.col("quantity_ordered") * F.col("unit_price"),
        )

        # ---- delivery variance (actual vs expected lead time) ----
        consumption = consumption.withColumn(
            "expected_delivery_date",
            F.date_add(F.col("order_date"), F.col("lead_time_days")),
        ).withColumn(
            "delivery_variance_days",
            F.datediff(F.col("received_date"), F.col("expected_delivery_date")),
        ).withColumn(
            "is_late",
            F.when(F.col("delivery_variance_days") > 0, True).otherwise(False),
        )

        # ---- per-material rolling 90-day spend ----
        window_90d = (
            Window
            .partitionBy("material_id")
            .orderBy(F.col("order_date").cast("long"))
            .rangeBetween(-90 * 86400, 0)
        )
        consumption = consumption.withColumn(
            "rolling_90d_spend",
            F.sum("line_cost").over(window_90d),
        )

        # ---- time features ----
        consumption = (
            consumption
            .withColumn("order_year", F.year("order_date"))
            .withColumn("order_month", F.month("order_date"))
            .withColumn("order_quarter", F.quarter("order_date"))
        )

        consumption = self.add_audit_columns(consumption)
        self.log_stats(consumption, "material_consumption")
        return {"material_consumption": consumption}


# ======================================================================
#  Supplier Performance — scorecard per supplier
# ======================================================================
class SupplierPerformanceTransformer(BaseTransformer):
    """
    Build a supplier scorecard by aggregating:
      • purchase_orders  (PostgreSQL)
      • raw_materials    (S3)
      • suppliers        (PostgreSQL)

    Produces one row per supplier with quality, cost, and delivery KPIs.
    """

    def transform(self, dfs: dict[str, DataFrame]) -> dict[str, DataFrame]:
        po = dfs["purchase_orders"]
        materials = dfs["raw_materials"]
        suppliers = dfs["suppliers"]

        po = self.deduplicate(po, ["po_id"])

        # ---- build base join ----
        base = (
            po
            .join(materials, on="material_id", how="left")
            .join(suppliers, on="supplier_id", how="left")
        )

        # ---- compute per-PO flags ----
        base = base.withColumn(
            "line_cost",
            F.col("quantity_ordered") * F.col("unit_price"),
        ).withColumn(
            "delivery_variance_days",
            F.datediff(F.col("received_date"),
                       F.date_add(F.col("order_date"), F.col("lead_time_days"))),
        ).withColumn(
            "is_late",
            F.when(F.col("delivery_variance_days") > 0, True).otherwise(False),
        ).withColumn(
            "qty_variance",
            F.col("quantity_received") - F.col("quantity_ordered"),
        )

        # ---- aggregate per supplier ----
        scorecard = (
            base
            .groupBy("supplier_id", "supplier_name")
            .agg(
                # volume & spend
                F.count("po_id").alias("total_orders"),
                F.sum("line_cost").alias("total_spend"),
                F.avg("line_cost").alias("avg_order_value"),
                F.countDistinct("material_id").alias("unique_materials"),

                # delivery
                F.avg("delivery_variance_days").alias("avg_delivery_variance_days"),
                F.sum(F.col("is_late").cast("int")).alias("late_deliveries"),
                F.avg(F.col("is_late").cast("int")).alias("late_delivery_rate"),

                # quantity accuracy
                F.avg("qty_variance").alias("avg_qty_variance"),
                F.sum(
                    F.when(F.col("qty_variance") < 0, 1).otherwise(0)
                ).alias("short_shipments"),

                # recency
                F.max("order_date").alias("last_order_date"),
                F.min("order_date").alias("first_order_date"),
            )
        )

        # ---- derived KPIs ----
        scorecard = scorecard.withColumn(
            "on_time_rate",
            F.lit(1.0) - F.col("late_delivery_rate"),
        ).withColumn(
            "relationship_tenure_days",
            F.datediff(F.current_date(), F.col("first_order_date")),
        ).withColumn(
            "supplier_tier",
            F.when(
                (F.col("on_time_rate") >= 0.95) & (F.col("total_spend") >= 100000),
                "strategic",
            )
            .when(F.col("on_time_rate") >= 0.85, "preferred")
            .when(F.col("on_time_rate") >= 0.70, "approved")
            .otherwise("under_review"),
        )

        scorecard = self.add_audit_columns(scorecard)
        self.log_stats(scorecard, "supplier_performance")
        return {"supplier_performance": scorecard}
