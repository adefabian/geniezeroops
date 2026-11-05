"""Small Delta helpers used by the job notebooks."""

from pyspark.sql import DataFrame, SparkSession


def upsert(spark: SparkSession, df: DataFrame, target: str, keys: list[str]) -> None:
    """MERGE df into target on keys; creates the table on first run."""
    if not spark.catalog.tableExists(target):
        df.write.saveAsTable(target)
        return

    view = "_upsert_source"
    df.createOrReplaceTempView(view)
    on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
    spark.sql(
        f"""
        MERGE INTO {target} t
        USING {view} s
        ON {on}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def overwrite(df: DataFrame, target: str) -> None:
    """Replace target (data and schema) with df."""
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
