"""
Read JSON schema files and convert them to PySpark ``StructType``.

Schema files follow a minimal convention (see config/schemas/*.json).
"""

from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

_TYPE_MAP: dict[str, type] = {
    "string": StringType,
    "integer": IntegerType,
    "long": LongType,
    "double": DoubleType,
    "float": DoubleType,
    "boolean": BooleanType,
    "date": DateType,
    "timestamp": TimestampType,
}


def load_schema(path: str | Path) -> StructType:
    """
    Parse a JSON schema file into a Spark ``StructType``.

    Parameters
    ----------
    path : path to the JSON schema file.
    """
    with open(path) as fh:
        raw = json.load(fh)

    fields = []
    for f in raw["fields"]:
        spark_type = _TYPE_MAP.get(f["type"].lower())
        if spark_type is None:
            raise ValueError(f"Unsupported type '{f['type']}' for field '{f['name']}'")
        fields.append(StructField(f["name"], spark_type(), f.get("nullable", True)))

    return StructType(fields)
