from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG     = spark.conf.get("bakeflow.catalog")
SRC_SCHEMA  = spark.conf.get("bakeflow.brz_schema")
TGT_SCHEMA = spark.conf.get("bakeflow.slv_schema")
SRC1_TABLE="b_media_customer_reviews"
SRC2_TABLE="b_media_gold_reviews_chunked"
TGT_TABLE="s_reviews"
### Expectations
re_rules={
    "r1": "franchise_id is not null",
    "r2": "review_date is not null"
}

dp.create_streaming_table(f"{CATALOG}.{TGT_SCHEMA}.{TGT_TABLE}",expect_all_or_drop=re_rules)

@dp.append_flow(target=f"{CATALOG}.{TGT_SCHEMA}.{TGT_TABLE}")
def customer_reviews():
    df=spark.readStream.table(f"{CATALOG}.{SRC_SCHEMA}.{SRC1_TABLE}")
    df=df.withColumnRenamed('franchiseID','franchise_id')
    df = df.withColumn("franchise_id", F.col("franchise_id").cast("int"))
    df = df.withColumn(
        "review_date",
        F.to_date(F.col("review_date"), "yyyy-MM-dd")
    )
    df = df.withColumn("brz_table", F.lit(f"{SRC1_TABLE}"))          
    df=df.select('franchise_id','review_date','review','brz_table','load_ts','batch_id')
    return df

@dp.append_flow(target=f"{CATALOG}.{TGT_SCHEMA}.{TGT_TABLE}")
def media_reviews():
    df=spark.readStream.table(f"{CATALOG}.{SRC_SCHEMA}.{SRC2_TABLE}")
    df=df.withColumnRenamed('franchiseID','franchise_id').withColumnRenamed('chunked_text','review')
    df = df.withColumn(
    "review_date",
    F.to_date(F.col("review_date"), "yyyy-MM-dd")
)
    df = df.withColumn("brz_table", F.lit(f"{SRC2_TABLE}"))          
    df=df.select('franchise_id','review_date','review','brz_table','load_ts','batch_id')
    return df