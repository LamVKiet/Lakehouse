"""
Shared Spark + Delta Lake + S3 + MySQL JDBC utilities.
All Spark jobs import from here to get a properly configured SparkSession
and consistent S3 path / MySQL connection helpers.
"""

import os

from pyspark.sql import SparkSession, DataFrame

S3_BUCKET = os.getenv("S3_BUCKET", "apparel-retail-lakehouse")
AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-1")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")

MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_DB   = os.getenv("MYSQL_DB", "apparel_retail")
MYSQL_USER = os.getenv("MYSQL_USER", "app_user")
MYSQL_PASS = os.getenv("MYSQL_PASSWORD", "app_pass")
JDBC_URL   = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?useSSL=false&serverTimezone=UTC"
JDBC_PROPS = {"user": MYSQL_USER, "password": MYSQL_PASS, "driver": "com.mysql.cj.jdbc.Driver"}


def get_spark_session(app_name: str) -> SparkSession:
    """Create a SparkSession configured for Delta Lake + S3A."""
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config("spark.hadoop.fs.s3a.endpoint.region", AWS_REGION)
    )

    # Pass AWS credentials via Spark Hadoop config so executors receive them
    # (env vars only exist on the driver container, not on worker containers)
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        builder = (builder
            .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
            .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        )
    else:
        builder = builder.config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
        )

    return builder.getOrCreate()


def get_s3_path(layer: str, table: str) -> str:
    """
    Build an S3A path for a Delta table.

    Examples:
        get_s3_path("bronze", "behavior_events")
        -> s3a://movie-ticketing-lakehouse/bronze/behavior_events/
    """
    return f"s3a://{S3_BUCKET}/{layer}/{table}/"


def read_delta(
    spark: SparkSession,
    layer: str,
    table: str,
    filter_col: str | None = None,
    filter_value: str | None = None,
) -> DataFrame:
    """
    Read a Delta table from S3.

    Args:
        spark: Active SparkSession.
        layer: Medallion layer — "bronze", "silver", or "gold".
        table: Table name — e.g. "behavior_events", "events", "funnel_hourly".
        filter_col: Optional partition column to filter (e.g. "log_date", "year_month").
        filter_value: Value to filter on (e.g. "2026-04-13", "202604").

    Returns:
        DataFrame with the requested data.

    Examples:
        # Read entire Silver table
        df = read_delta(spark, "silver", "events")

        # Read Bronze for a specific date
        df = read_delta(spark, "bronze", "behavior_events",
                        filter_col="log_date", filter_value="2026-04-13")

        # Read Gold aggregation for a month
        df = read_delta(spark, "gold", "funnel_hourly",
                        filter_col="year_month", filter_value="202604")
    """
    path = get_s3_path(layer, table)
    df = spark.read.format("delta").load(path)

    if filter_col and filter_value:
        from pyspark.sql.functions import col
        df = df.filter(col(filter_col) == filter_value)

    return df


def write_delta_append(df: DataFrame, path: str, partition_by: str | list[str] | None = "log_date") -> None:
    """Append rows to a Delta table (used for Bronze — immutable landing zone).
    Pass partition_by=None for low-cardinality dims (no physical partitioning)."""
    writer = df.write.format("delta").mode("append")
    if partition_by is not None:
        parts = [partition_by] if isinstance(partition_by, str) else list(partition_by)
        writer = writer.partitionBy(*parts)
    writer.save(path)


def write_delta_merge(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    merge_keys: list[str],
    partition_by: list[str] | str | None = "log_date",
    update_on_match: bool = False,
    update_exclude_cols: list[str] | None = None,
    table_properties: dict | None = None,
) -> None:
    """
    Merge (upsert) rows into a Delta table.
      - whenNotMatchedInsertAll() always.
      - whenMatchedUpdateAll() if update_on_match=True (SCD1 dim, mutable fact).
      - update_exclude_cols: when update_on_match=True, columns listed here are
        preserved on UPDATE (e.g. created_at — original creation timestamp).
        Insert path still writes them as-is.
    Idempotent, safe to re-run.
    On first run, falls back to overwrite + applies table_properties (CDF, DV, etc.).
    """
    from delta.tables import DeltaTable

    if DeltaTable.isDeltaTable(spark, path):
        condition = " AND ".join(f"t.{k} = s.{k}" for k in merge_keys)
        builder = DeltaTable.forPath(spark, path).alias("t").merge(df.alias("s"), condition)
        if update_on_match:
            if update_exclude_cols:
                update_set = {c: f"s.{c}" for c in df.columns if c not in update_exclude_cols}
                builder = builder.whenMatchedUpdate(set=update_set)
            else:
                builder = builder.whenMatchedUpdateAll()
        builder.whenNotMatchedInsertAll().execute()
    else:
        writer = df.write.format("delta").mode("overwrite")
        if partition_by is not None:
            parts = [partition_by] if isinstance(partition_by, str) else list(partition_by)
            writer = writer.partitionBy(*parts)
        if table_properties:
            for k, v in table_properties.items():
                writer = writer.option(k, v)
        writer.save(path)


def ensure_constraint(spark: SparkSession, path: str, name: str, expression: str) -> None:
    """ALTER TABLE ADD CONSTRAINT — idempotent (ignore if already exists)."""
    try:
        spark.sql(f"ALTER TABLE delta.`{path}` ADD CONSTRAINT {name} CHECK ({expression})")
        print(f"[constraint] Added: {name}")
    except Exception as e:
        msg = str(e).lower()
        if "already exists" in msg or "constraint" in msg and "exists" in msg:
            return
        raise


def enable_cdf(spark: SparkSession, path: str) -> None:
    """Enable Change Data Feed on a Delta table (idempotent)."""
    spark.sql(
        f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
    )


def enable_deletion_vectors(spark: SparkSession, path: str) -> None:
    """Enable Deletion Vectors on a Delta table (Delta 3.0+, idempotent)."""
    spark.sql(
        f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES (delta.enableDeletionVectors = true)"
    )


def register_glue_table(
    spark: SparkSession,
    database: str,
    table: str,
    path: str,
) -> None:
    """
    Register a Delta table in AWS Glue Data Catalog via boto3.
    Creates the Glue database first if it does not exist.
    Safe to call multiple times — create falls back to update if already exists.
    """
    import boto3
    from botocore.exceptions import ClientError

    glue = boto3.client("glue", region_name=AWS_REGION)

    # Trino uses native S3 client (s3://), not Hadoop S3A (s3a://)
    glue_location = path.replace("s3a://", "s3://").rstrip("/")

    # Create Glue database if not exists
    try:
        glue.create_database(DatabaseInput={"Name": database})
    except ClientError as e:
        if e.response["Error"]["Code"] != "AlreadyExistsException":
            raise

    # Derive column list from the Delta table schema
    df = spark.read.format("delta").load(path)
    columns = [
        {"Name": field.name, "Type": field.dataType.simpleString()}
        for field in df.schema.fields
    ]

    table_input = {
        "Name": table,
        "TableType": "EXTERNAL_TABLE",
        "StorageDescriptor": {
            "Columns": columns,
            "Location": glue_location,
            "InputFormat": "org.apache.hadoop.mapred.SequentialFileInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {"path": glue_location},
            },
        },
        "Parameters": {
            "spark.sql.sources.provider": "delta",
            "classification": "delta",
            "location": glue_location,
        },
    }

    try:
        glue.create_table(DatabaseName=database, TableInput=table_input)
    except ClientError as e:
        if e.response["Error"]["Code"] == "AlreadyExistsException":
            glue.update_table(DatabaseName=database, TableInput=table_input)
        else:
            raise

    print(f"Glue table registered: {database}.{table} -> {path}")


def write_delta_replace_partition(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    partition_col: str,
    partition_value: str,
    partition_by: list[str] | str | None = None,
) -> None:
    """
    Overwrite a single partition in a Delta table (used for Gold — T-1 day).
    Idempotent: re-running replaces exactly the same partition.
    On first run, falls back to a full overwrite with multi-level partitioning.
    """
    from delta.tables import DeltaTable
    if DeltaTable.isDeltaTable(spark, path):
        (df.write.format("delta").mode("overwrite").option("replaceWhere", f"{partition_col} = '{partition_value}'").save(path))
    else:
        if partition_by is None:
            parts = [partition_col]
        elif isinstance(partition_by, str):
            parts = [partition_by]
        else:
            parts = list(partition_by)
        (df.write.format("delta").mode("overwrite").partitionBy(*parts).save(path))


# ─────────────────────────────────────────────────────────────────────────────
# MySQL JDBC
# ─────────────────────────────────────────────────────────────────────────────

def read_mysql(spark: SparkSession, table: str, where: str | None = None) -> DataFrame:
    """Read a MySQL table via JDBC with optional pushdown WHERE clause."""
    query = f"(SELECT * FROM {table}{f' WHERE {where}' if where else ''}) AS t"
    return spark.read.jdbc(url=JDBC_URL, table=query, properties=JDBC_PROPS)


def read_mysql_date(spark: SparkSession, table: str, date_col: str, date_str: str) -> DataFrame:
    """Read rows where DATE(date_col) = date_str (T-1 pushdown)."""
    return read_mysql(spark, table, where=f"DATE({date_col}) = '{date_str}'")


def read_mysql_incremental(
    spark: SparkSession, table: str, date_str: str,
    date_col: str = "COALESCE(updated_at, created_at)",
) -> DataFrame:
    """Incremental read: rows where DATE(COALESCE(updated_at, created_at)) = date_str."""
    return read_mysql(spark, table, where=f"DATE({date_col}) = '{date_str}'")


# ─────────────────────────────────────────────────────────────────────────────
# SCD2 MERGE (products / branches)
# ─────────────────────────────────────────────────────────────────────────────

def write_scd2_merge(spark: SparkSession, new_df: DataFrame, path: str, key_col: str) -> None:
    """
    SCD2 upsert into a Delta table:
      1. Close active records whose non-key columns changed (set is_current = false).
      2. Insert new/changed records with is_current = true.
    On first run (no Delta table yet) falls back to full overwrite.
    """
    from delta.tables import DeltaTable
    import pyspark.sql.functions as f

    if not DeltaTable.isDeltaTable(spark, path):
        new_df.write.format("delta").mode("overwrite").save(path)
        return
    target = DeltaTable.forPath(spark, path)
    (
        target.alias("t")
        .merge(new_df.alias("s"), f"t.{key_col} = s.{key_col} AND t.is_current = true")
        .whenMatchedUpdate(condition="t.updated_at < s.updated_at", set={"is_current": "false"})
        .execute()
    )
    active_keys = target.toDF().filter(f.col("is_current") == True).select(key_col)
    insert_df = new_df.join(active_keys, on=key_col, how="left_anti")
    if insert_df.count() > 0:
        insert_df.write.format("delta").mode("append").save(path)
