from pyspark import pipelines as dp
from pyspark.sql import functions as F, Window

CATALOG     = spark.conf.get("bakeflow.catalog")
SRC_SCHEMA  = spark.conf.get("bakeflow.slv_schema")
TGT_SCHEMA = spark.conf.get("bakeflow.gld_schema")
SRC_TABLE="s_reviews"
TGT_TABLE="fact_reviews"

# ---------- fact helpers ----------
def franchise_lookup():
    return (spark.read.table(f"{CATALOG}.{TGT_SCHEMA}.dim_franchises")
                 .select("franchise_id", "franchise_key", "start_at", "end_at")
                 .alias("fr"))

# ---------- fact_reviews (append only) ----------
@dp.table(name=f"{CATALOG}.{TGT_SCHEMA}.{TGT_TABLE}")
def fact_reviews():
    r = (spark.readStream.table(f"{CATALOG}.{SRC_SCHEMA}.{SRC_TABLE}")
              .withColumn("review_ts", F.col("review_date").cast("timestamp"))
              .alias("r"))
    joined = r.join(
        franchise_lookup(),
        (F.col("r.franchise_id") == F.col("fr.franchise_id"))
        & (F.col("r.review_ts") >= F.col("fr.start_at"))
        & (F.col("fr.end_at").isNull() | (F.col("r.review_ts") < F.col("fr.end_at"))),
        "left",
    ).select("r.*", "fr.franchise_key")
    return (
        joined
        .withColumn("franchise_key", F.coalesce("franchise_key", F.lit(-1)))
        .withColumn("date_key", F.date_format("review_ts", "yyyyMMdd").cast("int"))
        .withColumn("insert_date", F.current_timestamp())
        .drop("franchise_id", "review_ts")
    )