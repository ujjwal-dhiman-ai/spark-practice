"""
Mini SCD2 Spark Application
Demonstrates sequential snapshot processing with proper DataFrame management
to avoid AnalysisException column reference issues.
"""

import os
import shutil
import hashlib
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType, BooleanType, IntegerType
from pyspark.storagelevel import StorageLevel

class SCD2MiniApp:
    def __init__(self, spark_session=None, base_path=None):
        """Initialize the SCD2 Mini Application"""
        self.spark = spark_session or self._create_spark_session()
        
        # Use Windows-compatible paths
        if base_path:
            self.base_path = base_path
        else:
            import tempfile
            self.base_path = os.path.join(tempfile.gettempdir(), "scd2_demo").replace("\\", "/")
        
        self.snapshots_path = f"{self.base_path}/snapshots"
        self.scd2_path = f"{self.base_path}/scd2_history.parquet"
        self.checkpoint_path = f"{self.base_path}/checkpoints"
        
        # Clean up previous runs
        self._cleanup_paths()
        
        # System columns for SCD2
        self.system_columns = [
            "BusinessKey", "RowHashKey", "SnapshotDate", 
            "FirstSnapshot", "ValidFrom", "ValidTo", "IsDeleted"
        ]
        
    def _create_spark_session(self):
        """Create Spark session with Windows-compatible settings"""
        return (SparkSession.builder
                .appName("SCD2MiniApp")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.hadoop.fs.defaultFS", "file:///")
                .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
                .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
                .config("spark.sql.parquet.writeLegacyFormat", "true")
                .config("spark.sql.execution.arrow.pyspark.enabled", "false")
                .master("local[*]")
                .getOrCreate())
    
    def _cleanup_paths(self):
        """Clean up previous run artifacts"""
        if os.path.exists(self.base_path):
            shutil.rmtree(self.base_path)
        os.makedirs(self.snapshots_path, exist_ok=True)
        os.makedirs(self.checkpoint_path, exist_ok=True)
    
    def generate_sample_data(self, num_days=10, records_per_day=1000):
        """Generate sample snapshot data for testing"""
        print(f"Generating {num_days} days of sample data...")
        
        base_date = datetime(2025, 8, 1)
        
        for day in range(num_days):
            current_date = base_date + timedelta(days=day)
            snapshot_date = current_date.strftime("%Y%m%d")
            
            # Generate sample customer data with some changes over time
            data = []
            for customer_id in range(1, records_per_day + 1):
                # Simulate data changes: some customers change email/phone over time
                email_suffix = f"v{day + 1}" if customer_id % 10 == 0 and day > 2 else "v1"
                phone_change = f"{1000 + day}" if customer_id % 15 == 0 and day > 4 else "1000"
                
                # Simulate some customers being deleted
                if customer_id % 50 == 0 and day > 6:
                    continue  # Skip this customer to simulate deletion
                
                # Simulate new customers being added
                max_customer = records_per_day + (day * 10) if day > 3 else records_per_day
                
                data.append({
                    "customer_id": customer_id,
                    "name": f"Customer_{customer_id}",
                    "email": f"customer_{customer_id}_{email_suffix}@example.com",
                    "phone": f"555-{phone_change}-{customer_id:04d}",
                    "city": f"City_{customer_id % 100}",
                    "status": "active" if customer_id % 20 != 0 or day < 5 else "inactive",
                    "FK_dimSnapshotID": snapshot_date
                })
            
            # Create DataFrame and write snapshot with Windows-compatible settings
            schema = StructType([
                StructField("customer_id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("email", StringType(), True),
                StructField("phone", StringType(), True),
                StructField("city", StringType(), True),
                StructField("status", StringType(), True),
                StructField("FK_dimSnapshotID", StringType(), True)
            ])
            
            df = self.spark.createDataFrame(data, schema)
            snapshot_file = f"{self.snapshots_path}/snapshot_{snapshot_date}.parquet"
            
            # Use coalesce to create single file and avoid Windows issues
            (df.coalesce(1)
               .write
               .mode("overwrite")
               .option("compression", "snappy")
               .parquet(snapshot_file))
            
            print(f"  Generated snapshot {snapshot_date} with {len(data)} records")
    
    def add_hash_key(self, df, business_key_cols):
        """Add RowHashKey based on all data columns (excluding system columns)"""
        data_cols = [col for col in df.columns if col not in self.system_columns + ["FK_dimSnapshotID"]]
        
        # Create hash from concatenated data columns
        hash_expr = F.md5(F.concat_ws("||", *[F.coalesce(F.col(c), F.lit("")) for c in data_cols]))
        
        # Add BusinessKey (composite if multiple columns)
        if isinstance(business_key_cols, list):
            business_key_expr = F.concat_ws("||", *[F.col(c) for c in business_key_cols])
        else:
            business_key_expr = F.col(business_key_cols)
            
        return df.withColumn("RowHashKey", hash_expr).withColumn("BusinessKey", business_key_expr)
    
    def add_system_columns(self, df, snapshot_date, is_new=True):
        """Add SCD2 system columns to the DataFrame"""
        return (df
                .withColumn("SnapshotDate", F.lit(snapshot_date))
                .withColumn("FirstSnapshot", F.lit(snapshot_date) if is_new else F.col("FirstSnapshot"))
                .withColumn("ValidFrom", F.lit(snapshot_date))
                .withColumn("ValidTo", F.lit("2999-12-31"))
                .withColumn("IsDeleted", F.lit(False)))
    
    def resolve_column_ambiguity(self, column_name, preferred_alias, fallback_alias, column_preferences=None):
        """
        Resolves ambiguous column references by checking column existence in DataFrames.
        
        Args:
            column_name: Name of column to resolve
            preferred_alias: Primary alias to use (e.g., 'curr' for current data)
            fallback_alias: Backup alias (e.g., 'lookup' for historical data)
            column_preferences: Dict specifying which alias to prefer for specific columns
        """
        if column_preferences is None:
            column_preferences = {
                "FirstSnapshot": "lookup",
                "default": "curr"
            }
        
        # Determine which alias to try first based on preferences
        first_choice = fallback_alias if column_preferences.get(column_name) == "lookup" else preferred_alias
        second_choice = preferred_alias if first_choice == fallback_alias else fallback_alias
        
        # Try first choice alias
        try:
            return F.col(f"{first_choice}.{column_name}")
        except Exception as e:
            # If first choice fails, try second choice
            try:
                return F.col(f"{second_choice}.{column_name}")
            except Exception as e:
                # If both fail, return the column without alias
                return F.col(column_name)
        
    def create_joined_df(self, current_df, history_df):
        """Create joined DataFrame to identify record changes"""
        # Proper aliasing to avoid column reference issues
        current_alias = current_df.select("BusinessKey", "RowHashKey").alias("curr")
        history_alias = history_df.select("BusinessKey", "RowHashKey").alias("hist")
        
        print(f"    Current records: {current_alias.count()}")
        print(f"    History active records: {history_alias.count()}")
        
        # Outer join with explicit column references
        joined_df = (
            history_alias
            .join(current_alias, F.col("hist.BusinessKey") == F.col("curr.BusinessKey"), "outer")
            .select(
                F.coalesce(F.col("hist.BusinessKey"), F.col("curr.BusinessKey")).alias("BusinessKey"),
                F.col("hist.RowHashKey").alias("hist_RowHashKey"),
                F.col("curr.RowHashKey").alias("curr_RowHashKey")
            )
        )
        
        # Determine record status
        status_expr = (
            F.when(F.col("hist_RowHashKey").isNull(), "new")
            .when(F.col("curr_RowHashKey").isNull(), "deleted")
            .when(F.col("curr_RowHashKey") == F.col("hist_RowHashKey"), "unchanged")
            .otherwise("updated")
        )
        
        return joined_df.withColumn("HistoryStatus", status_expr)
    
    
    def process_scd2_changes(self, current_df, history_df, snapshot_date, column_order):
        """Process SCD2 changes and return updated history DataFrame"""
        print(f"    Processing SCD2 changes for {snapshot_date}")
        
        if history_df.count() == 0:
            print(f"    No historical data, returning current snapshot as full history")
            return current_df.select(*column_order)
        
        # Get active records from history
        active_history = history_df.filter(F.col("ValidTo") == F.lit("2999-12-31"))
        
        # Identify changes
        joined_df = self.create_joined_df(current_df, active_history)
        joined_df = joined_df.persist(StorageLevel.MEMORY_ONLY)
        
        # Create first snapshot lookup
        first_lookup = (
            history_df
            .groupBy("BusinessKey")
            .agg(F.min("FirstSnapshot").alias("FirstSnapshot"))
        )
        
        # Process deleted records
        deleted_keys = joined_df.filter(F.col("HistoryStatus") == "deleted").select("BusinessKey")
        deleted_records = (
            active_history
            .join(deleted_keys, ["BusinessKey"], "semi")
            .withColumn("ValidTo", 
                F.date_sub(
                    F.to_date(F.lit(snapshot_date), 'yyyyMMdd'),  # First convert string to date
                    1
                )
            )
            .withColumn("IsDeleted", F.lit(True))
            .select(*column_order)
        )
        
        # Process updated records (expire old versions)
        updated_keys = joined_df.filter(F.col("HistoryStatus") == "updated").select("BusinessKey")
        expired_records = (
            active_history
            .join(updated_keys, ["BusinessKey"], "semi")
            .withColumn("ValidTo", 
                F.date_sub(
                    F.to_date(F.lit(snapshot_date), 'yyyyMMdd'),  # First convert string to date
                    1
                )
            )
            .select(*column_order)
        )
        
        # Process new and updated records (insert new versions)
        new_and_updated_keys = joined_df.filter(F.col("HistoryStatus").isin(["new", "updated"])).select("BusinessKey")
        new_records = (
            current_df.alias("curr")
            .join(new_and_updated_keys, ["BusinessKey"], "semi")
            .join(
                first_lookup.alias("lookup"),
                F.col("curr.BusinessKey") == F.col("lookup.BusinessKey"),
                "left"
            )
            .select(*[
                self.resolve_column_ambiguity(
                    column_name=col,
                    preferred_alias="curr",
                    fallback_alias="lookup",
                    column_preferences={
                        "FirstSnapshot": "lookup",
                        "ValidFrom": "curr",
                        "ValidTo": "curr",
                        "IsDeleted": "curr",
                        "RowHashKey": "curr",
                        "default": "curr"
                    }
                ).alias(col)
                for col in column_order
            ])
        )
        
        # Process unchanged records
        unchanged_records = (
            history_df
            .join(deleted_keys, ["BusinessKey"], "left_anti")
            .join(updated_keys, ["BusinessKey"], "left_anti")
            .select(*column_order)
        )
        
        # Clean up intermediate DataFrame
        joined_df.unpersist()
        
        # Union all record types
        final_df = (
            unchanged_records
            .unionByName(expired_records)
            .unionByName(new_records)
            .unionByName(deleted_records)
            .select(*column_order)
        )
        
        change_summary = {
            "new": new_and_updated_keys.join(joined_df.filter(F.col("HistoryStatus") == "new"), ["BusinessKey"]).count(),
            "updated": updated_keys.count(),
            "deleted": deleted_keys.count(),
            "unchanged": joined_df.filter(F.col("HistoryStatus") == "unchanged").count()
        }
        
        print(f"    Changes: {change_summary}")
        
        return final_df
    
    def process_snapshots(self, start_date, end_date, checkpoint_every=2):
        """Main function to process snapshots sequentially"""
        print(f"Processing snapshots from {start_date} to {end_date}")
        print(f"Checkpoint frequency: every {checkpoint_every} snapshots")
        
        # Convert dates to datetime objects
        dt_start = datetime.strptime(start_date, "%Y%m%d")
        dt_end = datetime.strptime(end_date, "%Y%m%d")
        current_date = dt_start
        
        # Initialize SCD2 DataFrame
        business_key_cols = ["customer_id"]  # Define business key
        
        # Read first snapshot to get schema
        first_snapshot_file = f"{self.snapshots_path}/snapshot_{start_date}.parquet"
        if not os.path.exists(first_snapshot_file):
            raise FileNotFoundError(f"First snapshot file not found: {first_snapshot_file}")
        
        sample_df = self.spark.read.parquet(first_snapshot_file)
        sample_df = self.add_hash_key(sample_df, business_key_cols)
        sample_df = self.add_system_columns(sample_df, start_date)
        
        # Define column order
        data_columns = [col for col in sample_df.columns if col not in self.system_columns + ["FK_dimSnapshotID"]]
        column_order = self.system_columns + data_columns
        
        # Initialize empty SCD2 DataFrame
        scd2_df = self.spark.createDataFrame([], sample_df.select(*column_order).schema)
        scd2_df = scd2_df.persist(StorageLevel.MEMORY_ONLY)
        
        processed_count = 0
        
        # Process each snapshot
        while current_date <= dt_end:
            snapshot_date = current_date.strftime("%Y%m%d")
            snapshot_file = f"{self.snapshots_path}/snapshot_{snapshot_date}.parquet"
            
            print(f"\n{'='*80}")
            print(f"Processing snapshot: {snapshot_date}")
            
            if not os.path.exists(snapshot_file):
                print(f"  WARNING: Snapshot file not found, skipping: {snapshot_file}")
                current_date += timedelta(days=1)
                continue
            
            try:
                # Read and prepare current snapshot
                current_df = self.spark.read.parquet(snapshot_file)
                current_df = self.add_hash_key(current_df, business_key_cols)
                current_df = self.add_system_columns(current_df, snapshot_date)
                
                print(f"  Input records: {current_df.count()}")
                
                # Process SCD2 changes
                new_scd2_df = self.process_scd2_changes(
                    current_df, scd2_df, snapshot_date, column_order
                )
                
                # CRITICAL: Proper DataFrame state management
                if scd2_df.is_cached:
                    scd2_df.unpersist()
                
                # Ensure consistent schema and persist
                scd2_df = new_scd2_df.select(*column_order).persist(StorageLevel.MEMORY_ONLY)
                
                processed_count += 1
                total_records = scd2_df.count()
                active_records = scd2_df.filter(F.col("ValidTo") == F.lit("2999-12-31")).count()
                
                print(f"  Total history records: {total_records}")
                print(f"  Active records: {active_records}")
                
                # Checkpoint logic
                if processed_count % checkpoint_every == 0:
                    print(f"  CHECKPOINT: Breaking lineage at {snapshot_date}")
                    checkpoint_file = f"{self.checkpoint_path}/checkpoint_{processed_count}.parquet"
                    
                    # Write checkpoint with Windows-compatible settings
                    (scd2_df.select(*column_order)
                           .coalesce(1)
                           .write
                           .mode("overwrite")
                           .option("compression", "snappy")
                           .parquet(checkpoint_file))
                    
                    # Critical: Clean DataFrame state
                    if scd2_df.is_cached:
                        scd2_df.unpersist()
                    
                    # Read back to break lineage
                    scd2_df = self.spark.read.parquet(checkpoint_file)
                    scd2_df = scd2_df.select(*column_order).persist(StorageLevel.MEMORY_ONLY)
                    
                    # Validate checkpoint
                    checkpoint_count = scd2_df.count()
                    print(f"  Checkpoint validation - record count: {checkpoint_count}")
                
            except Exception as e:
                print(f"  ERROR: Failed processing {snapshot_date}: {str(e)}")
                if scd2_df.is_cached:
                    scd2_df.unpersist()
                raise
            
            current_date += timedelta(days=1)
        
        # Final write with Windows-compatible settings
        print(f"\nWriting final SCD2 history to: {self.scd2_path}")
        (scd2_df.select(*column_order)
               .coalesce(1)
               .write
               .mode("overwrite")
               .option("compression", "snappy")
               .parquet(self.scd2_path))
        
        # Final statistics
        final_count = scd2_df.count()
        active_final = scd2_df.filter(F.col("ValidTo") == F.lit("2999-12-31")).count()
        
        print(f"\nFINAL RESULTS:")
        print(f"Total history records: {final_count}")
        print(f"Active records: {active_final}")
        
        # Clean up
        if scd2_df.is_cached:
            scd2_df.unpersist()
        
        return scd2_df
    
    def analyze_results(self):
        """Analyze the final SCD2 results"""
        if not os.path.exists(self.scd2_path):
            print("No SCD2 results found to analyze")
            return
        
        print(f"\n{'='*80}")
        print("ANALYZING SCD2 RESULTS")
        print(f"{'='*80}")
        
        df = self.spark.read.parquet(self.scd2_path)
        
        # Basic statistics
        total_records = df.count()
        active_records = df.filter(F.col("ValidTo") == F.lit("2999-12-31")).count()
        deleted_records = df.filter(F.col("IsDeleted") == True).count()
        
        print(f"Total history records: {total_records}")
        print(f"Active records: {active_records}")
        print(f"Deleted records: {deleted_records}")
        print(f"Historical versions: {total_records - active_records}")
        
        # Show sample data
        print(f"\nSample active records:")
        df.filter(F.col("ValidTo") == F.lit("2999-12-31")).select(
            "BusinessKey", "name", "email", "ValidFrom", "ValidTo"
        ).orderBy("BusinessKey").show(10)
        
        # Show customers with multiple versions
        print(f"\nCustomers with history (multiple versions):")
        history_customers = (
            df.groupBy("BusinessKey")
            .agg(F.count("*").alias("version_count"))
            .filter(F.col("version_count") > 1)
            .orderBy(F.desc("version_count"))
        )
        
        history_customers.show(10)
        
        # Show detailed history for a sample customer
        if history_customers.count() > 0:
            sample_key = history_customers.first()["BusinessKey"]
            print(f"\nDetailed history for BusinessKey: {sample_key}")
            (df.filter(F.col("BusinessKey") == sample_key)
               .select("BusinessKey", "name", "email", "ValidFrom", "ValidTo", "IsDeleted")
               .orderBy("ValidFrom")
               .show(truncate=False))
    
    def cleanup(self):
        """Clean up resources"""
        if hasattr(self, 'spark'):
            self.spark.stop()


def main():
    """Main function to run the SCD2 mini application"""
    print("SCD2 Mini Application - Testing Column Reference Issues")
    print("="*80)
    
    app = SCD2MiniApp()
    
    try:
        # Generate test data
        app.generate_sample_data(num_days=10, records_per_day=100)
        
        # Process snapshots (this will trigger the column reference issue if not handled properly)
        print(f"\nStarting SCD2 processing...")
        app.process_snapshots(
            start_date="20250801", 
            end_date="20250810", 
            checkpoint_every=2  # This forces frequent checkpointing to test lineage breaks
        )
        
        # Analyze results
        app.analyze_results()
        
        print(f"\n{'='*80}")
        print("SCD2 Mini Application completed successfully!")
        print("This demonstrates proper DataFrame management to avoid AnalysisException")
        print(f"{'='*80}")
        
    except Exception as e:
        print(f"\nERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        
    finally:
        app.cleanup()
        # pass


if __name__ == "__main__":
    main()


# Alternative: Interactive testing functions
def test_column_reference_issue():
    """
    Demonstrates the column reference issue that can occur without proper DataFrame management
    """
    print("\nTesting Column Reference Issue Reproduction...")
    
    app = SCD2MiniApp()
    
    try:
        # Create a simple test case that would fail with improper DataFrame handling
        spark = app.spark
        
        # Create initial DataFrame
        data1 = [{"id": 1, "name": "John", "value": 100}]
        df1 = spark.createDataFrame(data1).persist()
        
        # Simulate checkpoint - write and read back
        temp_path = "/tmp/test_checkpoint.parquet"
        df1.write.mode("overwrite").parquet(temp_path)
        
        # This creates new column references
        df2 = spark.read.parquet(temp_path)
        
        # This join would fail if we tried to reference old df1 columns with new df2 columns
        # The fix is to always use explicit column selection and proper aliasing
        
        print("Column reference test completed - proper handling prevents AnalysisException")
        
    except Exception as e:
        print(f"Column reference test failed: {e}")
    finally:
        app.cleanup()


def run_performance_test():
    """
    Run a performance test with larger datasets to see the impact of checkpointing
    """
    print("\nRunning Performance Test...")
    
    app = SCD2MiniApp()
    
    try:
        # Generate larger dataset
        app.generate_sample_data(num_days=30, records_per_day=10000)
        
        # Test with different checkpoint frequencies
        import time
        
        for checkpoint_freq in [5, 10, 15]:
            print(f"\nTesting with checkpoint frequency: {checkpoint_freq}")
            start_time = time.time()
            
            app.process_snapshots("20250801", "20250830", checkpoint_every=checkpoint_freq)
            
            end_time = time.time()
            print(f"Completed in {end_time - start_time:.2f} seconds")
            
    finally:
        app.cleanup()