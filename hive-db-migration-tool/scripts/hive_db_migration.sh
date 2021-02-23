#!/bin/bash
#Source Database name, example sourcedb
SOURCE_DB_NAME=
#Target Database name, example targetdb
TARGET_DB_NAME=
#Source Database master host, example 10.3.20.198
SOURCE_DB_MASTER_HOST=
#Source Database metastore port, example 9083
SOURCE_DB_METASTORE_PORT=
#Source Database beeline port, example 10000
SOURCE_DB_BEELINE_PORT=
#Source Database hive username, example hive
SOURCE_DB_HIVE_USERNAME=
#Source Database hive password, example hive
SOURCE_DB_HIVE_PASSWORD=
#Target Database master host, example 10.3.20.152
TARGET_DB_MASTER_HOST=
#Target Database metastore port, example 9083
TARGET_DB_METASTORE_PORT=
#Name of csv file, example migrationopportunity.csv
TABLE_CSV_FILE_NAME=
#S3 Path for target database, example s3://hive-db-migration/targetdb.db
TARGET_DATABASE_S3_DIR=
#Access control list value, example authenticated-read
ACL=authenticated-read
#Specify tables in comma separated format to migrate selected tables only instead of whole database
MIGRATE_TABLES=
# DynamoDB Metadata Table Name
METADATA_TABLE_NAME=
#set RUN_EMRFS_SYNC to true to run EMRFS Sync, it should be set to false
RUN_EMRFS_SYNC=false
# Tables split for parallel execution of splits
TABLESSPLIT=

echo "DB Migration process started for source database ${SOURCE_DB_NAME} to target database ${TARGET_DB_NAME} at $(date)"
echo "Going to create tables from source database ${SOURCE_DB_NAME} to target database ${TARGET_DB_NAME} at $(date)"
java -DsourceDbName=${SOURCE_DB_NAME} -DtargetDbName=${TARGET_DB_NAME} -DsourceDbMetastoreHost=${SOURCE_DB_MASTER_HOST} -DsourceDbMetastorePort=${SOURCE_DB_METASTORE_PORT}  -DtargetDbMetastoreHost=${TARGET_DB_MASTER_HOST}  -DtargetDbMetastorePort=${TARGET_DB_METASTORE_PORT} -DtableCsvFileName=${TABLE_CSV_FILE_NAME} -DmigrateTables=${MIGRATE_TABLES} -DtargetDatabaseS3Dir=${TARGET_DATABASE_S3_DIR} -DdynamoDbTableName=${METADATA_TABLE_NAME} -jar ssi-rst-hive-db-migration-tool.jar

TABLE_CREATE_STATUS=$?
TABLES_COUNT_CSV=$(cat $TABLE_CSV_FILE_NAME | wc -l)

TOTAL_LINES=$(wc -l <${TABLE_CSV_FILE_NAME})
((LINES_PER_FILE = ($TOTAL_LINES + ${TABLESSPLIT} - 1) / ${TABLESSPLIT}))

### Spliting splits file into given number of files
split --lines=${LINES_PER_FILE} ${TABLE_CSV_FILE_NAME} migration_split_

echo "Total lines     = ${TOTAL_LINES}"
echo "Lines per file = ${LINES_PER_FILE}"
wc -l "migration_splits_*"

Files=$(find . -iname "migration_split_*")
echo "files are $Files"

submitJob() {
   	echo "$split"
   	#Code for migration
    IFS=","
    if [ $TABLE_CREATE_STATUS -eq 0 ]; then
		while read f1 f2 f3 f4
		do
			tableName=$f1
			sourceLocation=$f2
			isPartitionedTable=$f3
			partitionColumnNames=$f4
			destinationLocation=$TARGET_DATABASE_S3_DIR/$tableName
			echo "Going to sync data for table ${tableName}  from source location ${sourceLocation} to destination location ${destinationLocation} at $(date)"
			aws s3 cp --acl $ACL ${sourceLocation} ${destinationLocation} --recursive
			S3_SYNC_RESULT=$?
			if [ $S3_SYNC_RESULT -eq 0 ]; then
				echo "Done syncing data for table ${tableName} on target database ${TARGET_DB_NAME} at $(date)"
				if [ "$RUN_EMRFS_SYNC" = true ] ; then
					echo "Running EMRFS Sync for table ${tableName} on ${destinationLocation} at $(date)"
					emrfs sync -m ${METADATA_TABLE_NAME} ${destinationLocation}
					EMRFS_SYNC_RESULT=$?
					if [ $EMRFS_SYNC_RESULT -eq 0 ]; then
						echo "EMRFS Sync completed for ${tableName} at $(date)"
					else
						echo "ERROR: Could Not run EMRFS Sync for ${tableName} at $(date)"
					fi
				fi

				if [ $isPartitionedTable == "yes" ]; then
					IFS=$'\n'
					echo "Getting Partitions from source database ${SOURCE_DB_NAME} for table ${tableName} at $(date)"
					read -a sourceTablePartitionArray <<<$(beeline -u jdbc:hive2://${SOURCE_DB_MASTER_HOST}:${SOURCE_DB_BEELINE_PORT}/${SOURCE_DB_NAME} --showHeader=false --outputformat=csv2 -n ${SOURCE_DB_HIVE_USERNAME} -p ${SOURCE_DB_HIVE_PASSWORD} -e "show partitions ${tableName};")
					IFS=$' '
					temparray=()
					for partition in ${sourceTablePartitionArray}
				    do
						partition="$(echo $partition | sed -e 's/[=]/=\"/g')"
					    partition="$(echo $partition | sed -e 's/[/]/\",/g')"
					    temparray+=("$partition")
				    done
				    partitionstr=""
					len=${#temparray[@]}
					for (( c=0; c<$len; c++ ))
					do
						mod=$(($c%100))
						if [[ $mod == 0 && $c != 0 ]]; then
							sudo -u hive hive --database "${TARGET_DB_NAME}" -e "ALTER TABLE ${tableName} ADD IF NOT EXISTS ${partitionstr};"
							partitionstr=""
						fi
						partitionstr="$partitionstr PARTITION (${temparray[c]}\")"
					done
					sudo -u hive hive --database "${TARGET_DB_NAME}" -e "ALTER TABLE ${tableName} ADD IF NOT EXISTS ${partitionstr};"
					echo "Source table partitions are:"
					for partition in "${sourceTablePartitionArray[@]}"
					do
						echo $partition
					done
					IFS=$'\n'
					read -a targetTablePartitionArray <<<$(sudo -u hive hive --database "${TARGET_DB_NAME}" -e "show partitions ${tableName};")
					echo "Target table partitions are:"
					for partition in "${targetTablePartitionArray[@]}"
					do
						echo $partition
					done
					IFS=$','
				    #	analyze compute for partitioned tables
				    partitionColumnNamesList=${partitionColumnNames//:/,}
				    echo "partitioned table name: ${tableName} and partitioned cols are:${partitionColumnNamesList}"
				    sudo -u hive hive --database "${TARGET_DB_NAME}" -e "ANALYZE TABLE ${tableName} PARTITION($partitionColumnNamesList) COMPUTE STATISTICS;"
            echo "Successfully migrated table ${tableName} at $(date)"

				else
					#	analyze compute for non-partitioned tables
				    sudo -u hive hive --database "${TARGET_DB_NAME}" -e "ANALYZE TABLE ${tableName} COMPUTE STATISTICS;"
					  echo "Skip adding partitions as ${tableName} on target database ${TARGET_DB_NAME} is not a partitioned table"
            echo "Successfully migrated table ${tableName} at $(date)"
				fi
      else
        echo "ERROR: sync data for table ${tableName} from source location ${sourceLocation} to destination location ${destinationLocation} at $(date)"
      fi
    done < $split
    else
      echo "ERROR: Unable to create tables from source database ${SOURCE_DB_NAME} to target database ${TARGET_DB_NAME} at $(date) "
    fi
}

for split in $Files
do
    echo "submitting job for split: $split"
    submitJob &
    sleep 5
done
wait

echo "DB Migration process completed for source database ${SOURCE_DB_NAME} to target database ${TARGET_DB_NAME} at $(date)"
