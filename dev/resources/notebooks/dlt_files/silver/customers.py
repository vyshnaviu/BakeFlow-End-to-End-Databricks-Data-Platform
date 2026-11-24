from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG     = spark.conf.get("bakeflow.catalog")
SRC_SCHEMA  = spark.conf.get("bakeflow.brz_schema")
TGT_SCHEMA = spark.conf.get("bakeflow.slv_schema")
SRC_TABLE="b_customers"
TGT_TABLE="s_customers"
cu_rules={
    "r1": "customer_id is not null"
}

dp.create_streaming_table(f"{CATALOG}.{TGT_SCHEMA}.{TGT_TABLE}",expect_all_or_drop=cu_rules)

@dp.view(name="s_customers_src")
def s_customers_src():
    return (
        spark.readStream.table(f"{CATALOG}.{SRC_SCHEMA}.{SRC_TABLE}")
            .withColumn("name", F.concat_ws(" ", F.col("first_name"), F.col("last_name")))
            .withColumnRenamed("customerID", "customer_id")
    )

dp.create_auto_cdc_flow(
    target=f"{CATALOG}.{TGT_SCHEMA}.{TGT_TABLE}",
    source="s_customers_src",
    keys=["customer_id"],
    sequence_by="file_modification_time",
    column_list=["customer_id", "name", "email_address", "phone_number", "address",
                 "city", "state", "country", "continent", "postal_zip_code",
                 "gender", "batch_id", "load_ts", "file_modification_time"],
    stored_as_scd_type="1",
)