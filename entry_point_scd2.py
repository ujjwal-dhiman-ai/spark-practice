dataGroupLoadLogs = lookupLoadLogs(inputLoadDetails, dataGroup)
logCount = dataGroupLoadLogs.count()

if logCount == 0:
    print(f"[INFO] No load logs found for {dataGroup}, skipping processing.")
    return

# Snapshot range
startingSnapshot = dataGroupLoadLogs.select(F.min("Snapshot")).first()[0]
endingSnapshot = dataGroupLoadLogs.select(F.max("Snapshot")).first()[0]

restored = False

try:
    if logCount == 1:
        # Single snapshot
        reloadExists = checkForReloadRequest(dataGroup, startingSnapshot)
        if reloadExists:
            print(f"[INFO] Reload request detected for snapshot {startingSnapshot}. Attempting history restore...")
            restored, latestSnapshot = tryRestoreHistory(
                dataGroup=dataGroup,
                dataTable=dataTable,
                tableMetadata=tableMetadataDF,
                strSnapshot=startingSnapshot,
                helperData=helperData
            )
        else:
            print(f"[INFO] No reload request for snapshot {startingSnapshot}. Skipping processing.")
            return

    else:
        # Multiple snapshots → always force restore from earliest
        print(f"[INFO] Multiple snapshots detected: {startingSnapshot} → {endingSnapshot}")
        print("[INFO] Forcing history restore from earliest snapshot...")
        restored, latestSnapshot = tryRestoreHistory(
            dataGroup=dataGroup,
            dataTable=dataTable,
            tableMetadata=tableMetadataDF,
            strSnapshot=startingSnapshot,
            helperData=helperData,
            forceRestore=True
        )

    # Process snapshots (single or multiple) only if history was restored
    if restored:
        print(f"[INFO] Processing snapshots from {startingSnapshot} to {endingSnapshot}...")        
        processAllSnapshots(
            dataGroup=dataGroup,
            dataTable=dataTable,
            firstSnapshot=startingSnapshot,
            lastSnapshot=endingSnapshot,
            tableMetadataDF=tableMetadataDF,
            helperData=helperData,
            dataGroupLoadLogs=dataGroupLoadLogs
        )
    else:
        print("[INFO] No changes detected after re-ingestion. Skipping snapshot processing.")

except Exception as e:
    print(f"[ERROR] Processing failed for {dataGroup}: {str(e)}")
    setDataTableGDPRStatus(status='Failed', logIDs=dataGroupLoadLogs)
    raise
