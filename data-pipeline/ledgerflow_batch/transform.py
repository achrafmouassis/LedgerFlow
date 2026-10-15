"""Pure Spark transformations, with monetary decimals and fail-fast checks."""
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


def aggregate_daily(facts):
    valid = (F.col("event_date").isNotNull()
             & F.col("currency").rlike("^[A-Z]{3}$")
             & F.col("operation_type").isin("OPENING", "DEPOSIT", "WITHDRAWAL", "TRANSFER")
             & F.col("amount").isNotNull() & (F.col("amount") > 0))
    if facts.filter(~F.coalesce(valid, F.lit(False))).limit(1).count():
        raise ValueError("Invalid analytical fact: date, currency, operation type or amount")
    # Decimal aggregation; no conversion to binary floating point.
    return facts.groupBy("event_date", "currency", "operation_type").agg(
        F.count(F.lit(1)).alias("operation_count"),
        F.sum(F.col("amount").cast(DecimalType(38, 2))).alias("total_amount"),
        F.min("amount").alias("min_amount"),
        F.max("amount").alias("max_amount"))
