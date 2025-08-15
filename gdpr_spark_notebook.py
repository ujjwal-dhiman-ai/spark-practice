import ast
import time
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType
from pyspark.storagelevel import StorageLevel

# Helper functions - Assume these are defined elsewhere
# getStagingColumns, getSensitiveColumns, addHashKey, detectSourceSignalAndAnonymise,
# addSystemColumns, captureDataChange, buildSchema, writeEmptyDataframe,
# getTableLoadType, getTableDataGDPRStatus, setDataGroupGDPRStatus, loadParquetFileIfExists

def processAllSnapshots(dataGroup, dataTable, firstSnapshot, lastSnapshot, tableMetadataDF, helperData, dataGroupLoadLogs):

    # Construct paths
    dataTablePath = f"{parquetBaseFilePath}/Bronze/Internal/{dataGroup}/{dataTable}/"
    scd2OutputPath = f"{parquetBaseFilePath}/Bronze/Internal/{dataGroup}/{dataTable}_scd2.parquet"
    scd2CheckpointPath = f"{parquetBaseFilePath}/Bronze/Internal/{dataGroup}/{dataTable}_scd2_checkpoint"

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
    except Exception:
        # If not, create an empty SCD2 dataframe and write it
        print(f"*[WRITE] Writing empty SCD2 while Initialization.")
        scd2DF = spark.createDataFrame([], buildSchema(columnOrder))
        scd2DF.select(columnOrder)
        scd2DF.write.mode("overwrite").parquet(scd2OutputPath)

    # Main processing loop for each snapshot

    while dtCurrentSnapshot <= dtLastSnapshot:

        try:
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
            
            dataLoadLogID = getTableDataGDPRStatus(dataGroupLoadLogID, dataTable, snapshotDate)
            
            print(f"=========================================================================")
            print(f"*[INFO] Processing for [{snapshotDate}] snapshot with ({snapshotDF.count()}) records.")
            
            # Set status to 'Running'
            setDataGroupGDPRStatus(status='Running', logIDs=dataGroupLoadLogID)
            setTableDataGDPRStatus(status='Running', logIDs=dataLoadLogID)
            
            parquetColumns = snapshotDF.columns
            if 'SnapshotDate' in parquetColumns:
                parquetColumns.remove('SnapshotDate')
            columnOrder = systemColumns + parquetColumns

            # Call the core processing function for a single snapshot
            status, scd2DF = processCurrentSnapshot(
                tableMetadata=tableMetadataDF,
                dataGroup=dataGroup,
                dataTable=dataTable,
                helperData=helperData,
                strSnapshot=snapshotDate,
                bronzeDF=snapshotDF,
                scd2DF=scd2DF
            )
            
            # Persist the new state to keep it in memory
            newState = scd2DF.persist(StorageLevel.MEMORY_ONLY)
            scd2DF = newState
            scd2DF.createOrReplaceTempView("scd2_history")

            processed += 1
            # Checkpoint logic to periodically write a small file and break lineage
            if processed % CHECKPOINT_EVERY == 0:
                print(f"*[INFO] Checkpoint: {snapshotDate}")
                checkpointPath = f"{scd2CheckpointPath}/checkpoint_{processed}.parquet"
                scd2DF.select(columnOrder).write.mode("overwrite").parquet(checkpointPath)

                # Read the checkpoint file back to break lineage
                scd2DF = spark.read.parquet(checkpointPath)
                scd2DF.persist(StorageLevel.MEMORY_ONLY)
                scd2DF.createOrReplaceTempView("scd2_history")

            # Error handling for the main loop
            
        except Exception as e:
            print("=========================================================================")
            setTableDataGDPRStatus(status='Failed', logIDs=dataLoadLogID)
            setDataGroupGDPRStatus(status='Failed', logIDs=dataGroupLoadLogID)
            raise RuntimeError(f"Failed ({dataTable})") from e

        print(f"Completed processing for {snapshotDate} snapshot in {time.time() - start_time}.")
        print("=========================================================================")
        dtCurrentSnapshot += timedelta(days=1)

    # Final write of the SCD2 history file
    scd2DF.write.mode("overwrite").parquet(scd2OutputPath)

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
                if not bronzeDF or bronzeDF.count() == 0:
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
            dataGroup=dataGroupName,
            dataTable=dataTableName,
            df=currentDF.select(columnOrder),
            scd2DF=scd2DF,
            sensitiveColumns=sensitiveColumns,
            outputPath=scd2OutputPath,
            helperData=helperData,
            isIncremental=(getTableLoadType(dataGroupName, dataTableName) == 'Incremental'),
            rtbfRecordIds=rtbfRecordIds,
            columnOrder=columnOrder
        )
        print(f"Captured change in {time.time() - start_time}.")

        print(f"*[SUCCESS] Finished processing {dataTableName} for {strSnapshot}.")
        return 'Successful', scd2DF

    except Exception as e:
        print(f"*[ERROR] Failed processing {dataTableName}: {e}")
        raise

# captureDataChange function definition
def captureDataChange(strSnapshot, dataGroup, dataTable, df, scd2DF, sensitiveColumns, outputPath, helperData, isIncremental, rtbfRecordIds, columnOrder):
    try:
        # Determine the BusinessKey column name
        if isinstance(ast.literal_eval(helperData['BusinessKey']), list):
            idCol = ast.literal_eval(helperData['BusinessKey'])[0]
        except:
            idCol = helperData['BusinessKey']

        historicalDF = scd2DF

        # If historical data is empty, the current dataframe is the full history
        if historicalDF.isEmpty():
            return df

        # Backfill any new columns in the historical data with Nulls
        historicalDF = backfillNewColumnInHistory(df=historicalDF, tableColumns=columnOrder)

        # Detect non-anonymized records in history that are now requested to be forgotten
        nonAnonymisedRTBFRecords = historicalDF.join(
            F.broadcast(rtbfRecords),
            historicalDF.col[idCol] == rtbfRecords[idCol],
            "semi"
        ).filter(historicalDF["GDPR_Compliant"] == "No")

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
        latestDF = historicalDF.filter(F.col("ValidTo") == F.lit("2999-12-31")).select(columnOrder)
        print(f"*[INFO] Total active records {latestDF.count()} )")
        print("print(\"current columns\", df.columns)")
        print("print(\"history columns\", latestDF.columns)")
        
        # Identify new, updated and deleted records by joining current and historical data
        joinedDF = createJoinedDF(df, latestDF, isIncremental)
        
        # Identify deleted records
        deletedRecords = (
            latestDF.join(
                F.broadcast(joinedDF.filter(F.col("HistoryStatus") == "deleted")),
                latestDF.col["BusinessKey"] == joinedDF.col["BusinessKey"],
                "left_semi"
            )
            .withColumn("ValidTo", F.date_sub(F.lit(strSnapshot), 1))
            .withColumn("IsDeleted", F.lit(True))
            .select(columnOrder)
        )
        
        # Identify records from history that are now updated
        expiredOld = (
            latestDF.join(
                F.broadcast(joinedDF.filter(F.col("HistoryStatus") == "updated")),
                latestDF.col["BusinessKey"] == joinedDF.col["BusinessKey"],
                "left_semi"
            )
            .withColumn("ValidTo", F.date_sub(F.lit(strSnapshot), 1))
            .select(columnOrder)
        )
        
        # Drop 'FirstSnapshot' column if it exists in the new data
        if "FirstSnapshot" in df.columns:
            df = df.drop("FirstSnapshot")
            
        # Identify new and updated records from today's data
        newRecords = (
            df.alias("currentDF")
            .join(
                F.broadcast(joinedDF.filter(F.col("HistoryStatus").isin("updated", "new"))),
                (F.col("curr.BusinessKey") == F.col("curr.BusinessKey")) & (F.col("curr.RowHashKey") == F.col("curr.RowHashKey")),
                "left_semi"
            )
            .join(
                firstLookup.alias("lookup"),
                F.col("BusinessKey") == F.col("lookup.BusinessKey"),
                "left"
            )
            .withColumn("FirstSnapshot", F.coalesce(F.col("lookup.FirstSnapshot"), F.lit(strSnapshot)))
        )
        
        # Identify unchanged records from history
        unchangedRecords = (
            historicalDF
            .join(deletedRecords.select("RowHashKey"), ["RowHashKey"], "left_anti")
            .join(expiredOld.select("RowHashKey"), ["RowHashKey"], "left_anti")
            .select(columnOrder)
        )

        # Combine all dataframes to create the final historical DF
        finalDF = unchangedRecords.unionByName(expiredOld).unionByName(newRecords).unionByName(deletedRecords)
        
        return finalDF

    except Exception as e:
        print(f"*[ERROR] Failed processing {dataTable}: {e}")
        raise RuntimeError(f"Failed ({dataTable})") from e


from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType
from pyspark.storagelevel import StorageLevel


def createJoinedDF(currentDF, historyDF, isIncremental):
    """
    Joins current and historical DataFrame on BusinessKey and determines record change status.

    Compares 'RowHashKey' values to identify if records are new, unchanged, updated, or deleted.
    Used in SCD2 logic to decide insert/update/delete operations.

    Args:
        currentDF (DataFrame): Latest snapshot of the data.
        historyDF (DataFrame): Historical SCD2 DataFrame.
        isIncremental (bool): Flag indicating load type is incremental.

    Returns:
        pyspark.sql.DataFrame: Joined DataFrame with comparison columns and record status.
    """
    joinCondition = "BusinessKey"
    joinType = "outer"

    print('current DF Keys', currentDF.select(joinCondition).show(10))
    print('history DF Keys', historyDF.select(joinCondition).show(10))

    joinedDF = (
        historyDF.alias("history")
        .join(F.broadcast(currentDF.alias("curr")), on=joinCondition, how=joinType)
        .select(
            F.col("history.BusinessKey"),
            F.col("curr.BusinessKey"),
            F.col("history.RowHashKey"),
            F.col("curr.RowHashKey")
        )
    )

    statusExpr = (
        F.when(F.col("history.BusinessKey").isNull(), "new")
        .when(F.col("curr.RowHashKey") == F.col("history.RowHashKey"), "unchanged")
        .when(F.col("curr.RowHashKey") != F.col("history.RowHashKey"), "updated")
    )

    if not isIncremental:
        statusExpr = statusExpr.when(F.col("curr.BusinessKey").isNull(), "deleted")

    return joinedDF.withColumn("HistoryStatus", statusExpr)

def createJoinedDF(currentDF, historyDF, isIncremental):
    """
    Joins current and historical DataFrame on BusinessKey and determines record change status using Spark SQL.

    Args:
        currentDF (DataFrame): Latest snapshot of the data.
        historyDF (DataFrame): Historical SCD2 DataFrame.
        isIncremental (bool): Flag indicating load type is incremental.

    Returns:
        pyspark.sql.DataFrame: Joined DataFrame with comparison columns and record status.
    """
    # Register temp views
    currentDF.select("BusinessKey", "RowHashKey").createOrReplaceTempView("curr")
    historyDF.select("BusinessKey", "RowHashKey").createOrReplaceTempView("history")

    # Spark SQL join and status logic
    sql = f"""
        SELECT
            COALESCE(history.BusinessKey, curr.BusinessKey) AS BusinessKey,
            history.RowHashKey AS history_RowHashKey,
            curr.RowHashKey AS curr_RowHashKey,
            CASE
                WHEN history.BusinessKey IS NULL THEN 'new'
                WHEN curr.RowHashKey = history.RowHashKey THEN 'unchanged'
                WHEN curr.RowHashKey != history.RowHashKey THEN 'updated'
                { "WHEN curr.BusinessKey IS NULL THEN 'deleted'" if not isIncremental else "" }
            END AS HistoryStatus
        FROM history
        FULL OUTER JOIN curr
            ON history.BusinessKey = curr.BusinessKey
    """

    joinedDF = spark.sql(sql)