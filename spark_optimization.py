from pyspark.sql import SparkSession, functions as F
from pyspark.storagelevel import StorageLevel
 
spark = SparkSession.builder.appName("SCD2_Sequential_Optimized").getOrCreate()
 
# --- CONFIG ---
snapshots_path_list = [
    "/mnt/data/snapshots/20240101.parquet",
    "/mnt/data/snapshots/20240201.parquet",
    # ...
]  # make this the ordered list of snapshots
final_parquet_path = "/mnt/data/final_scd2/"
checkpoint_dir = "/mnt/data/scd2_checkpoints/"  # must be reachable (HDFS/ADLS/etc)
 
business_key = "id"                     # natural/business key
business_cols = ["id", "col1", "col2"]  # columns from snapshot used to compare
eff_start_col = "eff_start_date"
eff_end_col = "eff_end_date"
is_current_col = "is_current"
 
# Persist/checkpoint frequency
CHECKPOINT_EVERY = 10   # break lineage every 10 snapshots (tune this)
 
# Initialize empty history schema (match snapshot schema + scd2 cols)
schema = """
    id STRING,
    col1 STRING,
    col2 STRING,
    eff_start_date DATE,
    eff_end_date DATE,
    is_current BOOLEAN
    """
scd2_df = spark.createDataFrame([], schema=schema)
 
# register view
scd2_df.createOrReplaceTempView("scd2_history")
 
# Helper: mark a snapshot row as SCD2 ready
def prepare_snapshot(snap_df, snapshot_date):
    return (
        snap_df.select(*business_cols)
                .withColumn(eff_start_col, F.lit(snapshot_date).cast("date"))
                .withColumn(eff_end_col, F.lit(None).cast("date"))
                .withColumn(is_current_col, F.lit(True))
            )
 
# Memory-friendly merge: uses anti-joins and small joins to reduce shuffle
def scd2_merge(history_df, snapshot_df, snapshot_date):
    """
    history_df: existing SCD2 DataFrame
    snapshot_df: new incoming snapshot (only business_cols)
    snapshot_date: date string
    """
    # 1) new incoming prepared
    snap_prepped = prepare_snapshot(snapshot_df, snapshot_date)
 
    # 2) identify keys in both sets
    hist_curr = history_df.filter(F.col(is_current_col) == True).select(*business_cols, eff_start_col, eff_end_col, is_current_col)
    hist_non_curr = history_df.filter(F.col(is_current_col) == False)
 
    # 3) rows present in history but not in snapshot => retain as-is (they might be past versions)
    #    (no join needed for these; they are hist_non_curr)
    retained_old = hist_non_curr
 
    # 4) find keys only in snapshot => pure new inserts
    snap_only = snap_prepped.join(hist_curr.select(business_key), on=business_key, how="left_anti")
    
        # 5) find keys only in history current but not in snapshot => mark current as closed (optionally keep as current=False)
    hist_only = hist_curr.join(snap_prepped.select(business_key), on=business_key, how="left_anti")\
                            .withColumn(eff_end_col, F.lit(snapshot_date).cast("date"))\
                            .withColumn(is_current_col, F.lit(False))
 
    # 6) keys present in both -> detect changes
    both_keys = snap_prepped.join(hist_curr, on=business_key, how="inner") \
                .select(
                    *[F.col(f"snap_prepped.{c}").alias(c) if c in snap_prepped.columns else F.col(c) for c in business_cols],
                    F.col(f"hist_curr.{eff_start_col}").alias(eff_start_col),
                    F.col(f"hist_curr.{eff_end_col}").alias(eff_end_col),
                    F.col(f"hist_curr.{is_current_col}").alias(is_current_col)
                )
 
    # To compare, join hist_curr and snap_prepped on business_key with suffixes
    joined = hist_curr.alias("h").join(snap_prepped.alias("s"), on=business_key, how="inner")
 
    # Detect changed rows: any business attribute differs
    neq_condition = None
    for c in business_cols:
        if c == business_key:
            continue
        cond = (F.col(f"h.{c}").isNull() & F.col(f"s.{c}").isNotNull()) | \
               (F.col(f"h.{c}").isNotNull() & F.col(f"s.{c}").isNull()) | \
               (F.col(f"h.{c}") != F.col(f"s.{c}"))
        neq_condition = cond if neq_condition is None else (neq_condition | cond)
 
    changed = joined.filter(neq_condition)
 
    # For changed: close current history, and insert new current from snapshot
    closed_changed = changed.select("h.*") \
            .withColumn(eff_end_col, F.lit(snapshot_date).cast("date")) \
            .withColumn(is_current_col, F.lit(False))
    
    new_for_changed = changed.select("s.*") \
            .withColumn(eff_start_col, F.lit(snapshot_date).cast("date")) \
            .withColumn(eff_end_col, F.lit(None).cast("date")) \
            .withColumn(is_current_col, F.lit(True))
    
        # For unchanged: keep the existing current rows as-is
    unchanged = joined.exceptAll(changed).select("h.*")  # safe because schemas align

    # union everything: past rows (retained_old), closed_changed, hist_only (closed), unchanged, new rows, new_for_changed
    out = (
        retained_old
        .unionByName(closed_changed)
        .unionByName(hist_only)
        .unionByName(unchanged)
        .unionByName(snap_only)
        .unionByName(new_for_changed)
    )
 
    # Optional: deduplicate (in case of overlaps)
    out = out.dropDuplicates([business_key, eff_start_col])
 
    # Return a compacted DataFrame (repartition by business key to colocate keys)
    return out.repartition(F.spark_partition_id())  # or use .repartition("id") to reduce key shuffles
 
# --- Processing loop with persistence & checkpointing ---
spark.sparkContext.setCheckpointDir(checkpoint_dir)
 
processed = 0
for snap_path in snapshots_path_list:
    snapshot_date = snap_path.split("/")[-1].replace(".parquet","")  # adapt to your filename scheme
    snap_df = spark.read.parquet(snap_path).select(*business_cols).distinct()
 
    # merge
    new_state = scd2_merge(scd2_df, snap_df, snapshot_date)
 
    # persist to keep in memory & allow reuse
    new_state = new_state.persist(StorageLevel.MEMORY_AND_DISK)
 
    # swap refs
    # unpersist old only AFTER new_state persisted to avoid losing cache
    old_state = scd2_df
    
    if old_state is not None:
        try:
            old_state.unpersist()
        except Exception:
            pass

    scd2_df = new_state
    scd2_df.createOrReplaceTempView("scd2_history")
 
    processed += 1
 
    # Break lineage / checkpoint and write small checkpoint file periodically
    if processed % CHECKPOINT_EVERY == 0:
        chk_path = f"{checkpoint_dir}/checkpoint_{processed}.parquet"
        scd2_df.write.mode("overwrite").parquet(chk_path)  # persistent checkpoint on storage
        # read it back to break lineage
        scd2_df = spark.read.parquet(chk_path)
        scd2_df = scd2_df.persist(StorageLevel.MEMORY_AND_DISK)
        scd2_df.createOrReplaceTempView("scd2_history")
 
# Final write
scd2_df.write.mode("overwrite").parquet(final_parquet_path)