import ast
import time
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType
from pyspark.storagelevel import StorageLevel

def setTableDataGDPRStatus(status, logIDs):
    raise NotImplementedError

def setDataGroupGDPRStatus(status, logIDs):
    raise NotImplementedError

# Helper functions - Assume these are defined elsewhere
# getStagingColumns, getSensitiveColumns, addHashKey, detectSourceSignalAndAnonymise,
# addSystemColumns, captureDataChange, buildSchema, writeEmptyDataframe,
# getTableLoadType, getTableDataGDPRStatus, setDataGroupGDPRStatus, loadParquetFileIfExists

def processAllSnapshots(dataGroup, dataTable, firstSnapshot, lastSnapshot, tableMetadataDF, helperData, dataGroupLoadLogs):

    # Construct paths
    dataTablePath = f"{parquetBaseFilePath}/Bronze/Internal/{dataGroup}/{dataTable}/{dataTable}.parquet/" # type: ignore
    scd2OutputPath = f"{parquetBaseFilePath}/Bronze/Internal/{dataGroup}/{dataTable}/{dataTable}_scd2.parquet" # type: ignore
    scd2CheckpointPath = f"{parquetBaseFilePath}/Bronze/Internal/{dataGroup}/{dataTable}/{dataTable}_scd2_checkpoint" # type: ignore

    # Read all snapshots from the specified path
    allSnapshotDF = spark.read.parquet(dataTablePath)
    # Filter for snapshots from the firstSnapshot date onwards
    allSnapshotDF = allSnapshotDF.filter(F.col("FK_dimSnapshotID") >= firstSnapshot)

    # Convert snapshot dates to datetime objects for iteration
    dtCurrentSnapshot = datetime.strptime(firstSnapshot, "%Y%m%d")
    dtLastSnapshot = datetime.strptime(lastSnapshot, "%Y%m%d")

    # Calculate the number of snapshots to process
    snapshotCount = (dtLastSnapshot - dtCurrentSnapshot).days + 1

    # Set checkpoint frequency
    if snapshotCount <= 2:
        CHECKPOINT_EVERY = snapshotCount
    else:
        CHECKPOINT_EVERY = 2

    # Prepare log IDs and set GDPR status to 'Queued'
    dataGroupLoadIDs = ", ".join([f"'{col}'" for col in [row['PK_DataGroupLoadLogID'] for row in dataGroupLoadLogs.select("PK_DataGroupLoadLogID").distinct().collect()]])
    setDataGroupGDPRStatus(status='Queued', logIDs=dataGroupLoadIDs)

    processed = 0

    # Define system columns and get parquet columns
    systemColumns = ["BusinessKey", "RowHashKey", "SnapshotDate", "FirstSnapshot", "ValidFrom", "ValidTo", "IsDeleted", "GDPR_Compliant"]
    parquetColumns = allSnapshotDF.columns
    if "FK_dimSnapshotID" in parquetColumns:
        parquetColumns.remove("FK_dimSnapshotID")
    columnOrder = systemColumns + parquetColumns

    # Initialize or read the SCD2 dataframe
    try:
        # Check if the output file already exists
        mssparkutils.fs.ls(scd2OutputPath)
        scd2DF = spark.read.parquet(scd2OutputPath)
        # CRITICAL: Ensure proper column ordering and caching after read
        scd2DF = scd2DF.select(*columnOrder).persist(StorageLevel.MEMORY_ONLY)
        scd2DF.createOrReplaceTempView("scd2_history")
    except Exception:
        # If not, create an empty SCD2 dataframe and write it
        print(f"*[WRITE] Writing empty SCD2 while Initialization.")
        scd2DF = spark.createDataFrame([], buildSchema(columnOrder))
        scd2DF = scd2DF.select(*columnOrder).persist(StorageLevel.MEMORY_ONLY)
        scd2DF.write.mode("overwrite").parquet(scd2OutputPath)
        scd2DF.createOrReplaceTempView("scd2_history")

    # Main processing loop for each snapshot
    while dtCurrentSnapshot <= dtLastSnapshot:
        print(f"=========================================================================")
        print(f"*[INFO] Processing for [{snapshotDate}] snapshot with ({snapshotDF.count()}) records.")
        start_time = time.time()
        snapshotDate = dtCurrentSnapshot.strftime("%Y%m%d")
        
        # Filter the current snapshot from the full dataframe
        snapshotDF = (
            allSnapshotDF
            .filter(F.col("FK_dimSnapshotID") == snapshotDate)
            .withColumnRenamed("FK_dimSnapshotID", "SnapshotDate")
        )
        
        # Get the log ID for the current snapshot
        dataGroupLoadLogID = (
            dataGroupLoadLogs
            .filter((F.col("DataGroupName") == dataGroup) & (F.col("SnapshotDate") == snapshotDate) & (F.col("Layer") == 'Bronze'))
            .select("PK_DataGroupLoadLogID")
            .first()[0]
        )
        dataLoadLogID = getTableDataGDPRStatus(dataGroupLoadLogID, dataTable, snapshotDate) # type: ignore
        # Set status to 'Running'
        setDataGroupGDPRStatus(status='Running', logIDs=dataGroupLoadLogID)
        setTableDataGDPRStatus(status='Running', logIDs=dataLoadLogID)
        
        parquetColumns = snapshotDF.columns
        if 'SnapshotDate' in parquetColumns:
            parquetColumns.remove('SnapshotDate')
        columnOrder = systemColumns + parquetColumns

        try:
            # Call the core processing function for a single snapshot
            status, newScd2DF = processCurrentSnapshot(
                tableMetadata=tableMetadataDF,
                dataGroup=dataGroup,
                dataTable=dataTable,
                helperData=helperData,
                strSnapshot=snapshotDate,
                bronzeDF=snapshotDF,
                scd2DF=scd2DF
            )
            
            # CRITICAL FIX: Properly handle DataFrame transitions
            # 1. Unpersist old DataFrame to free memory
            if scd2DF.is_cached:
                scd2DF.unpersist()
                
            # 2. Ensure consistent column ordering and persist new state
            scd2DF = newScd2DF.select(*columnOrder).persist(StorageLevel.MEMORY_ONLY)
            
            # 3. Create temp view with fresh DataFrame reference
            scd2DF.createOrReplaceTempView("scd2_history")

            processed += 1
            
            # Checkpoint logic to periodically write and break lineage
            if processed % CHECKPOINT_EVERY == 0:
                print(f"*[INFO] Checkpoint: {snapshotDate}")
                checkpointPath = f"{scd2CheckpointPath}/checkpoint_{processed}.parquet"
                
                # Write checkpoint with explicit column selection
                scd2DF.select(*columnOrder).write.mode("overwrite").parquet(checkpointPath)
                
                # CRITICAL FIX: Clean DataFrame state management
                # 1. Unpersist current DataFrame
                if scd2DF.is_cached:
                    scd2DF.unpersist()
                    
                # 2. Read checkpoint to break lineage with fresh column references
                scd2DF = spark.read.parquet(checkpointPath)
                
                # 3. Ensure consistent schema and persist
                scd2DF = scd2DF.select(*columnOrder).persist(StorageLevel.MEMORY_ONLY)
                
                # 4. Recreate temp view with clean state
                scd2DF.createOrReplaceTempView("scd2_history")
                
                # 5. Force materialization to ensure clean state
                print(f"*[INFO] Checkpoint validation - record count: {scd2DF.count()}")

        except Exception as e:
            print("=========================================================================")
            print(f"*[ERROR] Processing failed for {snapshotDate}: {str(e)}")
            setTableDataGDPRStatus(status='Failed', logIDs=dataLoadLogID)
            setDataGroupGDPRStatus(status='Failed', logIDs=dataGroupLoadLogID)
            
            # Clean up resources on failure
            if 'scd2DF' in locals() and scd2DF.is_cached:
                scd2DF.unpersist()
                
            raise RuntimeError(f"Failed ({dataTable}) at snapshot {snapshotDate}") from e

        print(f"*[SUCCESS] Completed processing for {snapshotDate} snapshot in {time.time() - start_time:.2f} seconds.")
        print("=========================================================================")
        dtCurrentSnapshot += timedelta(days=1)

    # Final write of the SCD2 history file
    print(f"*[WRITE] Writing final SCD2 output to {scd2OutputPath}")
    scd2DF.select(*columnOrder).write.mode("overwrite").parquet(scd2OutputPath)
    
    # Clean up final resources
    if scd2DF.is_cached:
        scd2DF.unpersist()

def getTableLoadType(dataGroup, dataTable):
    raise NotImplementedError

def addSystemColumns(isNew, df, helperData, strSnapshot):
    raise NotImplementedError

def detectSourceSignalAndAnonymise(bronzeDF, dataGroup, dataTable, businessKey, pattern, anonymisationSignalField, columns):
    raise NotImplementedError

def addHashKey(keyName, df):
    raise NotImplementedError

def getSensitiveColumns(tableMetadata, dataGroup, dataTable, parquetColumns=None):
    raise NotImplementedError

def writeEmptyDataframe(schema, outputPath):
    raise NotImplementedError

def buildSchema(columns):
    raise NotImplementedError

def loadParquetFileIfExists(path, dataGroup, dataTable, snapshot):
    raise NotImplementedError

def getStagingColumns(dataGroup, dataTable):
    raise NotImplementedError

# processCurrentSnapshot function definition
def processCurrentSnapshot(tableMetadata, dataGroup, dataTable, helperData, strSnapshot=None, bronzeDF=None, scd2DF=None):
    layer = 'Bronze'
    isNew = False if bronzeDF is None else True

    # Construct paths
    bronzeBasePath = f"{tableMetadata['parquetBasePath']}/Bronze/Internal/{dataGroup}/{dataTable}"
    silverBasePath = f"{tableMetadata['parquetBasePath']}/Silver/Internal/{dataGroup}/{dataTable}"
    snapshotPath = f"{bronzeBasePath}/parquet/{helperData['fk_dimSnapshotID']}/{strSnapshot}.parquet"
    scd2OutputPath = f"{bronzeBasePath}_scd2.parquet"
    keyFilepathBronze = f"{bronzeBasePath}_key_file.parquet/{helperData['fk_dimSnapshotID']}/{strSnapshot}.parquet"
    keyFilepathSilver = f"{silverBasePath}_key_file.parquet/{helperData['fk_dimSnapshotID']}/{strSnapshot}.parquet"

    try:
        stagingColumns = getStagingColumns(dataGroup, dataTable)
        systemColumns = ["BusinessKey", "RowHashKey", "SnapshotDate", "FirstSnapshot", "ValidFrom", "ValidTo", "IsDeleted", "GDPR_Compliant"]

        # Reading bronze file
        if bronzeDF is None:
            if loadParquetFileIfExists(
                path=snapshotPath,
                dataGroup=dataGroup,
                dataTable=dataTable,
                snapshot=strSnapshot
            ):
                pass
            else:
                if not bronzeDF or bronzeDF.isEmpty() == 0:
                    print(f"*[SKIP] Skipping table {dataTable} as no snapshot file was found.")
                    try:
                        mssparkutils.fs.ls(scd2OutputPath)
                    except Exception:
                        print(f"*[WRITE] Input data is empty, writing empty SCD2 and Key File.")
                        writeEmptyDataframe(buildSchema(systemColumns + stagingColumns), scd2OutputPath)
                        writeEmptyDataframe(buildSchema(["RowHashKey", "SnapshotDate"]), keyFilepathBronze)
                        writeEmptyDataframe(buildSchema(["RowHashKey", "SnapshotDate"]), keyFilepathSilver)
                    return 'Skip', spark.createDataFrame([], buildSchema(systemColumns + stagingColumns))

        parquetColumns = bronzeDF.columns
        if 'SnapshotDate' in parquetColumns:
            parquetColumns.remove('SnapshotDate')
        columnOrder = systemColumns + parquetColumns

        # Get column partitions
        print(f"*[INFO] Partitioning Sensitive and Non-sensitive columns.")
        sensitiveColumns = getSensitiveColumns(tableMetadata, dataGroup, dataTable, parquetColumns=parquetColumns)

        # Add rowhashkey as unique key to join back sensitive and key files before staging
        print(f"*[INFO] Adding RowHashKey.")
        bronzeDF = addHashKey(keyName="RowHashKey", df=bronzeDF)

        # WRITE bronze key file.
        print(f"*[WRITE] Write bronze key file.")
        keyDF = bronzeDF.select("RowHashKey").withColumn("SnapshotDate", F.lit(strSnapshot))
        keyDF.write.mode("overwrite").parquet(keyFilepathBronze)

        # WRITE silver key file
        print(f"*[WRITE] Write silver key file.")
        keyDF.write.mode("overwrite").parquet(keyFilepathSilver)

        # Detect source signal and anonymise
        print(f"*[INFO] Detect source signals and anonymise current data.")
        anonymizedDF, rtbfRecordIds = detectSourceSignalAndAnonymise(
            bronzeDF,
            dataGroup,
            dataTable,
            businessKey = helperData['BusinessKey'],
            pattern = helperData['anonymisationPattern'],
            anonymisationSignalField = helperData['anonymisationSignalColumn'],
            columns=sensitiveColumns
        )

        # Add ValidTo, ValidFrom, and other system columns
        print(f"*[INFO] Adding system columns to bronze dataframe.")
        currentDF = addSystemColumns(
            isNew,
            df=anonymizedDF,
            helperData=helperData,
            strSnapshot=strSnapshot
        )

        # Start change tracking
        start_time = time.time()
        print(f"*[INFO] Capturing change and maintaining sensitive history file.")
        scd2DF = captureDataChange(
            strSnapshot=strSnapshot,
            dataGroup=dataGroup,  # Fixed variable name reference
            dataTable=dataTable,  # Fixed variable name reference
            df=currentDF.select(*columnOrder),  # Explicit column selection
            scd2DF=scd2DF,
            sensitiveColumns=sensitiveColumns,
            outputPath=scd2OutputPath,
            helperData=helperData,
            isIncremental=(getTableLoadType(dataGroup, dataTable) == 'Incremental'),  # Fixed variable names
            rtbfRecordIds=rtbfRecordIds,
            columnOrder=columnOrder
        )
        print(f"*[SUCCESS] Captured change in {time.time() - start_time:.2f} seconds.")

        print(f"*[SUCCESS] Finished processing {dataTable} for {strSnapshot}.")
        return 'Successful', scd2DF

    except Exception as e:
        print(f"*[ERROR] Failed processing {dataTable}: {e}")
        raise

# captureDataChange function definition - MAJOR FIXES
def captureDataChange(strSnapshot, dataGroup, dataTable, df, scd2DF, sensitiveColumns, outputPath, helperData, isIncremental, rtbfRecordIds, columnOrder):
    try:
        # Determine the BusinessKey column name
        try:
            if isinstance(ast.literal_eval(helperData['BusinessKey']), list):
                idCol = ast.literal_eval(helperData['BusinessKey'])[0]
            else:
                idCol = helperData['BusinessKey']
        except:
            idCol = helperData['BusinessKey']

        # CRITICAL FIX: Create fresh DataFrame reference to avoid stale column references
        historicalDF = scd2DF.select(*columnOrder)

        # If historical data is empty, the current dataframe is the full history
        if historicalDF.count() == 0:  # Use count() instead of isEmpty() for better reliability
            print(f"*[INFO] No historical data found, returning current snapshot as full history")
            return df.select(*columnOrder)

        # Backfill any new columns in the historical data with Nulls
        historicalDF = backfillNewColumnInHistory(df=historicalDF, tableColumns=columnOrder)

        # CRITICAL FIX: Handle RTBF records with proper DataFrame management
        if rtbfRecordIds is not None and not rtbfRecordIds.rdd.isEmpty():
            # Create a broadcast DataFrame for RTBF records to optimize joins
            rtbfRecords = F.broadcast(rtbfRecordIds.select(idCol).distinct())
            
            # Detect non-anonymized records in history that are now requested to be forgotten
            nonAnonymisedRTBFRecords = (
                historicalDF
                .join(rtbfRecords, historicalDF[idCol] == rtbfRecords[idCol], "semi")
                .filter(F.col("GDPR_Compliant") == "No")
            )

            if not nonAnonymisedRTBFRecords.rdd.isEmpty():
                print(f"*[INFO] Non-anonymised RTBF records detected in SCD2 file, anonymising History")
                # Apply anonymization to the historical data
                historicalDF = anonymiseHistory(
                    dataGroup=dataGroup,
                    dataTable=dataTable,
                    df=historicalDF,
                    nonAnonymisedRTBFRecords=nonAnonymisedRTBFRecords,
                    rtbfRecords=rtbfRecords,
                    sensitiveColumns=sensitiveColumns,
                    idCol=idCol
                )
        
        # Get the latest active records from the historical data
        latestDF = historicalDF.filter(F.col("ValidTo") == F.lit("2999-12-31")).select(*columnOrder)
        print(f"*[INFO] Total active records: {latestDF.count()}")
        
        # CRITICAL FIX: Identify new, updated and deleted records with proper DataFrame handling
        joinedDF = createJoinedDF(df.select(*columnOrder), latestDF, isIncremental)
        
        # Cache the joined DataFrame since it will be used multiple times
        joinedDF = joinedDF.persist(StorageLevel.MEMORY_ONLY)
        
        # Get first snapshot lookup for new records
        firstLookup = (
            historicalDF
            .groupBy("BusinessKey")
            .agg(F.min("FirstSnapshot").alias("FirstSnapshot"))
        )
        
        # Identify deleted records
        deletedRecords = (
            latestDF
            .join(
                joinedDF.filter(F.col("HistoryStatus") == "deleted").select("BusinessKey"),
                ["BusinessKey"],
                "semi"
            )
            .withColumn("ValidTo", F.date_sub(F.lit(strSnapshot), 1))
            .withColumn("IsDeleted", F.lit(True))
            .select(*columnOrder)
        )
        
        # Identify records from history that are now updated
        expiredOld = (
            latestDF
            .join(
                joinedDF.filter(F.col("HistoryStatus") == "updated").select("BusinessKey"),
                ["BusinessKey"],
                "semi"
            )
            .withColumn("ValidTo", F.date_sub(F.lit(strSnapshot), 1))
            .select(*columnOrder)
        )
        
        # Identify new and updated records from today's data
        newAndUpdatedKeys = joinedDF.filter(F.col("HistoryStatus").isin(["updated", "new"])).select("BusinessKey")
        
        newRecords = (
            df.select(*columnOrder)
            .join(newAndUpdatedKeys, ["BusinessKey"], "semi")
            .join(firstLookup, ["BusinessKey"], "left")
            .withColumn("FirstSnapshot", 
                       F.coalesce(F.col("FirstSnapshot"), F.lit(strSnapshot)))
            .select(*columnOrder)
        )
        
        # Identify unchanged records from history
        unchangedRecords = (
            historicalDF
            .join(deletedRecords.select("BusinessKey"), ["BusinessKey"], "left_anti")
            .join(expiredOld.select("BusinessKey"), ["BusinessKey"], "left_anti")
            .select(*columnOrder)
        )

        # Clean up intermediate cached DataFrames
        joinedDF.unpersist()

        # Combine all dataframes to create the final historical DF
        finalDF = (
            unchangedRecords
            .unionByName(expiredOld)
            .unionByName(newRecords)
            .unionByName(deletedRecords)
            .select(*columnOrder)  # Ensure consistent column ordering
        )
        
        return finalDF

    except Exception as e:
        print(f"*[ERROR] Failed processing {dataTable}: {e}")
        raise RuntimeError(f"Failed ({dataTable})") from e


def createJoinedDF(currentDF, historyDF, isIncremental):
    """
    Joins current and historical DataFrame on BusinessKey and determines record change status.
    
    FIXED VERSION: Addresses column reference issues by using proper aliasing and explicit column selection.
    """
    joinCondition = "BusinessKey"
    
    # CRITICAL FIX: Create properly aliased DataFrames with explicit column selection
    currentAlias = currentDF.select("BusinessKey", "RowHashKey").alias("curr")
    historyAlias = historyDF.select("BusinessKey", "RowHashKey").alias("hist")
    
    print(f'*[DEBUG] Current DF Keys count: {currentAlias.count()}')
    print(f'*[DEBUG] History DF Keys count: {historyAlias.count()}')

    # Perform outer join with explicit column references
    joinedDF = (
        historyAlias
        .join(currentAlias, F.col("hist.BusinessKey") == F.col("curr.BusinessKey"), "outer")
        .select(
            F.coalesce(F.col("hist.BusinessKey"), F.col("curr.BusinessKey")).alias("BusinessKey"),
            F.col("hist.RowHashKey").alias("hist_RowHashKey"),
            F.col("curr.RowHashKey").alias("curr_RowHashKey")
        )
    )

    # Create status logic with clearer conditions
    statusExpr = (
        F.when(F.col("hist_RowHashKey").isNull(), "new")
        .when(F.col("curr_RowHashKey").isNull(), 
              F.when(F.lit(isIncremental), "unchanged").otherwise("deleted"))
        .when(F.col("curr_RowHashKey") == F.col("hist_RowHashKey"), "unchanged")
        .otherwise("updated")
    )

    return joinedDF.withColumn("HistoryStatus", statusExpr)


def backfillNewColumnInHistory(df, tableColumns):
    """
    Helper function to backfill new columns in historical DataFrame with nulls.
    Ensures schema consistency between current and historical data.
    """
    historyColumns = df.columns
    newColumns = [col for col in tableColumns if col not in historyColumns]
    
    if newColumns:
        print(f"*[INFO] Backfilling new columns in history: {newColumns}")
        for col in newColumns:
            df = df.withColumn(col, F.lit(None).cast(StringType()))
    
    return df.select(*tableColumns)


def anonymiseHistory(dataGroup, dataTable, df, nonAnonymisedRTBFRecords, rtbfRecords, sensitiveColumns, idCol):
    """
    Anonymizes historical records that are marked for RTBF (Right to be Forgotten).
    This is a placeholder - implement actual anonymization logic based on your requirements.
    """
    print(f"*[INFO] Anonymizing {nonAnonymisedRTBFRecords.count()} historical records")
    
    # Placeholder implementation - replace with actual anonymization logic
    # This should apply your anonymization pattern to sensitive columns
    anonymizedDF = df  # Implement your anonymization logic here
    
    return anonymizedDF