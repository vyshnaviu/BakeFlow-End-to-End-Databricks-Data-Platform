from pyspark import pipelines as dp
from pyspark.sql import functions as F, Window
CATALOG     = spark.conf.get("bakeflow.catalog")
SRC_SCHEMA  = spark.conf.get("bakeflow.slv_schema")
TGT_SCHEMA = spark.conf.get("bakeflow.gld_schema")
SRC_TABLE="s_customers"
TGT_TABLE="dim_customer"
# ---------- dim_customer (SCD1) ----------
@dp.materialized_view(name=f"{CATALOG}.{TGT_SCHEMA}.{TGT_TABLE}")
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
def dim_customer():
    return (
        spark.read.table(f"{CATALOG}.{SRC_SCHEMA}.{SRC_TABLE}")
             .withColumn("customer_key", F.xxhash64("customer_id"))
    )
