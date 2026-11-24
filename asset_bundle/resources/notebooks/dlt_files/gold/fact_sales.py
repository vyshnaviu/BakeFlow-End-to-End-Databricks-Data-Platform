from pyspark import pipelines as dp
from pyspark.sql import functions as F, Window

CATALOG     = spark.conf.get("bakeflow.catalog")
SRC_SCHEMA  = spark.conf.get("bakeflow.slv_schema")
TGT_SCHEMA = spark.conf.get("bakeflow.gld_schema")
SRC_TABLE="s_transactions"
TGT_TABLE="fact_sales"

# ---------- fact helpers ----------
def franchise_lookup():
    return (spark.read.table(f"{CATALOG}.{TGT_SCHEMA}.dim_franchises")
                 .select("franchise_id", "franchise_key", "start_at", "end_at")
                 .alias("fr"))


# ---------- fact_sales (append only) ----------
@dp.table(name=f"{CATALOG}.{TGT_SCHEMA}.{TGT_TABLE}")
@dp.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
@dp.expect("franchise_found", "franchise_key <> -1")
def fact_sales():
    t = spark.readStream.table(f"{CATALOG}.{SRC_SCHEMA}.{SRC_TABLE}").alias("t")
    joined = t.join(
        franchise_lookup(),
        (F.col("t.franchise_id") == F.col("fr.franchise_id"))
        & (F.col("t.paid_time") >= F.col("fr.start_at"))
        & (F.col("fr.end_at").isNull() | (F.col("t.paid_time") < F.col("fr.end_at"))),
        "left",
    ).select("t.*", "fr.franchise_key")
    return (
        joined
        .withColumn("customer_key", F.xxhash64("customer_id"))
        .withColumn("franchise_key", F.coalesce("franchise_key", F.lit(-1)))
        .withColumn("date_key", F.date_format("paid_time", "yyyyMMdd").cast("int"))
        .withColumn("insert_date", F.current_timestamp())
        .drop("customer_id", "franchise_id")
    )
