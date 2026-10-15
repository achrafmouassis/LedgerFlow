"""Daily batch orchestration: snapshot -> Spark -> stage -> atomic publish."""
import json
import os
from uuid import UUID
from pyspark.sql import SparkSession, functions as F
from pyspark import StorageLevel
from .config import DatabaseSettings, DateRange
from .transform import aggregate_daily
from .store import exclusive_batch, start_run, capture_snapshot, publish_run, fail_run


def create_spark():
    builder = (SparkSession.builder.appName("LedgerFlowDailySummary")
        .master(os.environ.get("SPARK_MASTER", "local[2]"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false"))
    driver = os.environ.get("JDBC_DRIVER_JAR")
    if driver:
        builder = builder.config("spark.jars", driver)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def jdbc_options(settings):
    return dict(url=settings.jdbc_url, user=settings.user, password=settings.password,
                driver="org.postgresql.Driver")


def run_batch(settings: DatabaseSettings, period: DateRange, spark=None):
    owns_spark = spark is None
    if owns_spark:
        spark = create_spark()
    try:
        with settings.connect() as connection, exclusive_batch(connection):
            run_id = start_run(connection, period)
            try:
                source_count = capture_snapshot(connection, run_id, period)
                # run_id is generated internally and formatted as a UUID, never raw SQL input.
                query = ("SELECT event_date,currency,operation_type,amount "
                         "FROM analytics.batch_source_snapshot WHERE run_id='" + str(UUID(str(run_id))) + "'")
                facts = (spark.read.format("jdbc").options(**jdbc_options(settings))
                    .option("query", query).option("fetchsize", 1000).load()
                    .persist(StorageLevel.MEMORY_AND_DISK))
                try:
                    daily = aggregate_daily(facts).withColumn("run_id", F.lit(str(run_id)))
                    (daily.write.format("jdbc").options(**jdbc_options(settings))
                        .option("dbtable", "analytics.daily_summary_stage")
                        .option("batchsize", 1000).mode("append").save())
                finally:
                    facts.unpersist()
                groups = publish_run(connection, run_id, period)
                return dict(run_id=str(run_id), status="SUCCEEDED", start=str(period.start),
                            end=str(period.end), source_count=source_count, group_count=groups)
            except Exception as error:
                fail_run(connection, run_id, error)
                raise
    finally:
        if owns_spark:
            spark.stop()
