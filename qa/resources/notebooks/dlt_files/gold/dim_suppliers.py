from pyspark import pipelines as dp
from pyspark.sql import functions as F, Window

CATALOG     = spark.conf.get("bakeflow.catalog")
SRC_SCHEMA  = spark.conf.get("bakeflow.slv_schema")
TGT_SCHEMA = spark.conf.get("bakeflow.gld_schema")
SRC_TABLE="s_suppliers"
TGT_TABLE="dim_suppliers"

# ---------- helper ----------
def scd2_prep(df, id_col):
    """SCD2 versions: audit columns plus start_at / end_at for point-in-time joins."""
    w = Window.partitionBy(id_col).orderBy("__START_AT")
    return (
        df.withColumn("insert_batch_id", F.col("batch_id"))
          .withColumn("update_batch_id", F.coalesce(F.lead("batch_id").over(w), F.col("batch_id")))
          .withColumn("insert_date", F.col("__START_AT"))
          .withColumn("update_date", F.coalesce(F.col("__END_AT"), F.col("__START_AT")))
          .withColumn("start_at",
                      F.when(F.row_number().over(w) == 1, F.lit("1900-01-01").cast("timestamp"))
                       .otherwise(F.col("__START_AT")))
          .withColumnRenamed("__END_AT", "end_at")
          .drop("__START_AT", "batch_id")
    )
# ---------- dim_supplier (SCD2) ----------
@dp.materialized_view(name=f"{CATALOG}.{TGT_SCHEMA}.{TGT_TABLE}")
@dp.expect_or_drop("valid_supplier_id", "supplier_id IS NOT NULL")
def dim_supplier():
    df = scd2_prep(spark.read.table(f"{CATALOG}.{SRC_SCHEMA}.{SRC_TABLE}"), "supplier_id")
    return df.withColumn("supplier_key", F.xxhash64("supplier_id", "start_at"))
