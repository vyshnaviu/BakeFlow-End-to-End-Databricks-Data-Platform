from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG     = spark.conf.get("bakeflow.catalog")
SRC_SCHEMA  = spark.conf.get("bakeflow.brz_schema")
TGT_SCHEMA = spark.conf.get("bakeflow.slv_schema")
SRC_TABLE="b_suppliers"
TGT_TABLE="s_suppliers"
sup_rules={
    "r1": "supplier_id is not null",
    "r2": "name is not null"
}

dp.create_streaming_table(f"{CATALOG}.{TGT_SCHEMA}.{TGT_TABLE}",expect_all_or_drop=sup_rules)
@dp.view()
def s_supplier_src():
    df=spark.readStream.table(f"{CATALOG}.{SRC_SCHEMA}.{SRC_TABLE}")
    df=df.withColumnRenamed('supplierID','supplier_id')                                                        
    return df


dp.create_auto_cdc_flow(
  target =f"{CATALOG}.{TGT_SCHEMA}.{TGT_TABLE}",
  source = "s_supplier_src",
  keys = ["supplier_id"],
  sequence_by = "file_modification_time",
  column_list = ["supplier_id","name","ingredient","continent","city","district","size",
"longitude","latitude","approved","batch_id","file_modification_time"], 
  stored_as_scd_type = "2", 
    track_history_except_column_list=["batch_id", "load_ts", "file_modification_time"]
    )
