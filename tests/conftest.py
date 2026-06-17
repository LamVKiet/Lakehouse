import os
import time

import pytest
from pyspark.sql import SparkSession

# Pin the driver timezone to match the Spark session tz so Python<->Spark timestamp
# conversions are deterministic on CI (ubuntu = UTC by default). time.tzset() is
# POSIX-only; on Windows it's a no-op (devs there are already +07), so date-level
# assertions in the suite stick to mid-day inputs that don't shift across midnight.
os.environ["TZ"] = "Asia/Ho_Chi_Minh"
if hasattr(time, "tzset"):
    time.tzset()


@pytest.fixture(scope="session")
def spark():
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("ci-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
    )
    # Load the Deequ jar when running DQ tests locally (DEEQU_LOCAL_JAR=path to
    # deequ-2.0.7-spark-3.5.jar). Harmless for the other suites; absent on CI -> DQ tests skip.
    deequ_jar = os.environ.get("DEEQU_LOCAL_JAR", "").strip()
    if deequ_jar:
        builder = builder.config("spark.jars", deequ_jar)
    s = builder.getOrCreate()
    s.sparkContext.setLogLevel("WARN")
    yield s
    s.stop()
