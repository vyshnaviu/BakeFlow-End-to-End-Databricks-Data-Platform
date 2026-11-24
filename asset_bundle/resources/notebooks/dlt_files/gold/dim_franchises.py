from pyspark import pipelines as dp
from pyspark.sql import functions as F, Window
CATALOG     = spark.conf.get("bakeflow.catalog")
SRC_SCHEMA  = spark.conf.get("bakeflow.slv_schema")
TGT_SCHEMA = spark.conf.get("bakeflow.gld_schema")
SRC_TABLE="s_franchises"
TGT_TABLE="dim_franchises"
FAR_FUTURE = F.lit("9999-12-31 00:00:00").cast("timestamp")


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


@dp.materialized_view(name=f"{CATALOG}.{TGT_SCHEMA}.{TGT_TABLE}")
@dp.expect_or_drop("valid_franchise_id", "franchise_id IS NOT NULL")
def dim_franchise():
    f = scd2_prep(spark.read.table(f"{CATALOG}.{SRC_SCHEMA}.{SRC_TABLE}"), "franchise_id").alias("f")
    s = (spark.read.table(f"{CATALOG}.{TGT_SCHEMA}.dim_suppliers")
            .select(
                F.col("supplier_id").alias("sup_id"),
                "supplier_key",
                F.col("start_at").alias("s_start"),
                F.col("end_at").alias("s_end"),
                F.col("insert_date").alias("s_insert_date"),
                F.col("update_date").alias("s_update_date"),
                F.col("insert_batch_id").alias("s_insert_batch"),
                F.col("update_batch_id").alias("s_update_batch"))
            .alias("s"))

    f_end = F.coalesce(F.col("f.end_at"), FAR_FUTURE)
    s_end = F.coalesce(F.col("s.s_end"), FAR_FUTURE)

    # join every franchise version to every supplier version that overlaps it in time
    joined = f.join(
        s,
        (F.col("f.supplier_id") == F.col("s.sup_id"))
        & (F.col("f.start_at") < s_end)
        & (F.col("s.s_start") < f_end),
        "left",
    )

    raw_end = F.least(f_end, s_end)
    is_current = raw_end == FAR_FUTURE
    supplier_starts = F.col("s.s_start") > F.col("f.start_at")   # the supplier change opened this row
    supplier_ends = s_end < f_end                                  # the supplier change closed this row

    ins_date = F.greatest(F.col("f.insert_date"), F.col("s.s_insert_date"))
    ins_batch = F.when(supplier_starts, F.col("s.s_insert_batch")).otherwise(F.col("f.insert_batch_id"))
    upd_date = (F.when(is_current, ins_date)
                 .when(supplier_ends, F.col("s.s_update_date"))
                 .otherwise(F.col("f.update_date")))
    upd_batch = (F.when(is_current, ins_batch)
                  .when(supplier_ends, F.col("s.s_update_batch"))
                  .otherwise(F.col("f.update_batch_id")))

    out = joined.select(
        "f.*",
        F.col("s.supplier_key").alias("supplier_key"),
        F.greatest(F.col("f.start_at"), F.col("s.s_start")).alias("_start"),
        F.when(is_current, F.lit(None).cast("timestamp")).otherwise(raw_end).alias("_end"),
        ins_date.alias("_ins_date"),
        upd_date.alias("_upd_date"),
        ins_batch.alias("_ins_batch"),
        upd_batch.alias("_upd_batch"),
    )

    return (
        out.drop("start_at", "end_at", "insert_date", "update_date",
                 "insert_batch_id", "update_batch_id")
           .withColumnRenamed("_start", "start_at")
           .withColumnRenamed("_end", "end_at")
           .withColumnRenamed("_ins_date", "insert_date")
           .withColumnRenamed("_upd_date", "update_date")
           .withColumnRenamed("_ins_batch", "insert_batch_id")
           .withColumnRenamed("_upd_batch", "update_batch_id")
           .withColumn("franchise_key", F.xxhash64("franchise_id", "start_at"))
    )