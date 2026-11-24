from pyspark import pipelines as dp
from pyspark.sql.functions import *

CATALOG     = spark.conf.get("bakeflow.catalog")
METADATA_SCHEMA=spark.conf.get("bakeflow.meta_schema")
M_BATCH="batch_control"
M_TABLE="table_info"
SRC_SCHEMA  = spark.conf.get("bakeflow.brz_schema")
TGT_SCHEMA = spark.conf.get("bakeflow.slv_schema")
SRC_TABLE="b_transactions"
TGT_TABLE="s_transactions"

batch_rows=spark.sql(f"""select batch_id from {CATALOG}.{METADATA_SCHEMA}.{M_BATCH} 
                    where batch_status='opened'""").collect()
max_batch=batch_rows[0]['batch_id'] if batch_rows else None

tra_rules={
    "r1": "transaction_id is not null",
    "r2": "customer_id is not null",
    "r3": "franchise_id is not null"

}
@dp.expect_all_or_drop(tra_rules)
@dp.table(name=f"{CATALOG}.{TGT_SCHEMA}.{TGT_TABLE}")
def s_supplier_src():
    df=spark.readStream.table(f"{CATALOG}.{SRC_SCHEMA}.{SRC_TABLE}")
    df=df.withColumnRenamed('transactionID','transaction_id')\
        .withColumnRenamed('franchiseID','franchise_id')\
            .withColumnRenamed('customerID','customer_id')\
                .withColumnRenamed('dateTime','paid_time')\
                    .withColumnRenamed('unitPrice','unit_price')\
                        .withColumnRenamed('totalPrice','total_price')\
                            .withColumnRenamed('cardNumber','card_number')\
                                .withColumnRenamed('paymentMethod','payment_method')
    df=df.select('transaction_id','franchise_id','customer_id','paid_time','product','quantity','unit_price','total_price','card_number','payment_method','load_ts','batch_id')
    return df

