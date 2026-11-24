from pyspark import pipelines as dp
from pyspark.sql import functions as F
CATALOG     = spark.conf.get("bakeflow.catalog")
SRC_SCHEMA  = spark.conf.get("bakeflow.brz_schema")
TGT_SCHEMA = spark.conf.get("bakeflow.slv_schema")
SRC_TABLE="b_franchises"
TGT_TABLE="s_franchises"
fr_rules={
    "r1": "franchise_id is not null",
    "r2": "name is not null"
}

dp.create_streaming_table(f"{CATALOG}.{TGT_SCHEMA}.{TGT_TABLE}",expect_all_or_drop=fr_rules)
@dp.view(name="s_franchises_src")
def s_customers_src():
    return (
        spark.readStream.table(f"{CATALOG}.{SRC_SCHEMA}.{SRC_TABLE}")
            .withColumnRenamed("franchiseID", "franchise_id")
            .withColumnRenamed("supplierID", "supplier_id")
    )

dp.create_auto_cdc_flow(
    target=f"{CATALOG}.{TGT_SCHEMA}.{TGT_TABLE}",
    source="s_franchises_src",
    keys=["franchise_id"],
    sequence_by="file_modification_time",
    column_list=["franchise_id", "name", "city", "district", "zipcode",
                 "country", "size", "longitude", "latitude", "supplier_id"
                 , "batch_id", "load_ts", "file_modification_time"],
    stored_as_scd_type="2",
    track_history_except_column_list=["batch_id", "load_ts", "file_modification_time"]
)