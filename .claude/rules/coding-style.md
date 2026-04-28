---
description: "Code conventions for Python, Spark, and Docker in this project"
---

# Coding Style

## Python

- Imports: group on one line if ≤ 4 items from the same module.
- Short args: keep inline on one line instead of breaking each onto its own.
- No docstrings for self-explanatory functions.
- Use f-strings instead of `.format()` or string concatenation.
- No unnecessary blank lines between statements in a function body.

## Spark

- Import `pyspark.sql.functions as f` (lowercase), not `as F`.
- Short `.select()` argx`x`s: inline on one line.
- Call `spark.conf.set()` immediately after `getOrCreate()`, before any DataFrame operations.

## ETL Spark Job File Structure

Every batch Spark job follows this 4-section layout (after imports + spark session):

```python
### imports + sys.path
### spark session (get_spark_session + conf.set)

### Section 1: functions
def transform():
    ...

### Section 2: params
ymd = sys.argv[1]
ym = ymd[0:7]
today = datetime.now().strftime("%Y-%m-%d")

print("PARAM >>>", ymd)
print("PARAM >>>", ym)
print("PARAM >>>", today)
print("PARAM >>>", datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))

run_date = ymd.replace("-", "")
run_month = ym.replace("-", "")
l1m_ym = (date(int(run_month[:4]), int(run_month[4:]), 1) - timedelta(days=1)).strftime("%Y%m")
SOME_PATH = get_s3_path("layer", "table")

### Section 3: run
transform()

### Section 4: stop
spark.stop()
```

- Functions are defined before params — they reference module-level globals set in Section 2.
- `run_date` = yyyyMMdd (for Delta partition filters).
- `run_month` = yyyyMM (for user_type month comparisons).
- `l1m_ym` = previous month yyyyMM (computed in pure Python, not Spark).
- S3 path constants (e.g. `NOU_PATH`, `SILVER_PATH`) are set in Section 2, used inside functions.

## Docker / Shell

- Use `docker compose` (v2 CLI plugin), not `docker-compose` (v1 standalone).
