### Loading all tables using Autoloader with dynamic path and streaming tables
### Use of streaming tables - append only (incremental),track changes(CDC) in delta tables and use checkpoint for idempotency(csv source)
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG     = spark.conf.get("bakeflow.catalog")
BRZ_SCHEMA  = spark.conf.get("bakeflow.brz_schema")
METADATA_SCHEMA = spark.conf.get("bakeflow.meta_schema")
M_BATCH = "batch_control"
M_TABLE = "table_info"

batch_rows=spark.sql(f"""select batch_id from {CATALOG}.{METADATA_SCHEMA}.{M_BATCH} 
                    where batch_status='opened'""").collect()
max_batch=batch_rows[0]['batch_id'] if batch_rows else None
FOLDER_PATH ="/Volumes/bakehouse_project/data_source/csv_source/bakehouse_dataset"
tables_path=f"{CATALOG}.{METADATA_SCHEMA}.{M_TABLE}"
res=spark.sql(f"select trim(table_name) as table_name from {tables_path}").collect()
TABLES=[x.table_name for x in res]
def all_tables_load(table):
    @dp.table(name=f'b_{table}')
    def brz_tables_load():
            return (
            spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("header", "true")
                .option("cloudFiles.inferColumnTypes", "true")
                .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                .load(f"{FOLDER_PATH}/{table}")
                .withColumn("load_ts", F.current_timestamp())
                .withColumn("batch_id", F.lit(max_batch))
                .withColumn("file_path",F.col("_metadata.file_path"))
                .withColumn("file_modification_time",F.col("_metadata.file_modification_time"))
        )

for table in TABLES:
    if max_batch is not None:
        all_tables_load(table)            



