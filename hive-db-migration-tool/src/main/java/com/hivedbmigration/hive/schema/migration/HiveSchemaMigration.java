package com.hivedbmigration.hive.schema.migration;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

import com.hivedbmigration.hive.schema.migration.constant.MigrationConstant;
import com.hivedbmigration.hive.schema.migration.exception.MigrationException;
import com.hivedbmigration.hive.schema.migration.utils.FileWriterUtil;
import com.hivedbmigration.hive.schema.migration.utils.HiveMetastoreUtil;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.log4j.Logger;

public class HiveSchemaMigration {

    private HiveMetaStoreClient hmsSourceDbClient = null;
    private HiveMetaStoreClient hmsTargetDbClient = null;
    private String sourceDbMetastoreHost = null;
    private String sourceDbMetastorePort = null;
    private String targetDbMetastoreHost = null;
    private String targetDbMetastorePort = null;
    private String sourceDbName = null;
    private String targetDbName = null;
    private String tableCsvFileName = null;
    private String migrateTables = null;
    private String targetDatabaseS3Dir = null;
    private String dynamoDbTableName = null;
    private FileWriter fwr = null;
    private final static Logger LOGGER = Logger.getLogger(HiveSchemaMigration.class.getName());

    public static void main(String a[]) {
        HiveSchemaMigration hsm = new HiveSchemaMigration();
        hsm.initialize();
        hsm.cleanUpDynamoDb();
        hsm.migrateDb();
        hsm.releaseResources();
    }

    private void releaseResources() {
        FileWriterUtil.closeFileWriter(fwr);
        HiveMetastoreUtil.closeHMSClient(hmsSourceDbClient);
        HiveMetastoreUtil.closeHMSClient(hmsTargetDbClient);
    }

    private void initialize() {
        LOGGER.info("Initializing hive schema creation process");
        sourceDbMetastoreHost = System.getProperty(MigrationConstant.PROP_SOURCE_DB_METASTORE_HOST, "");
        sourceDbMetastorePort = System.getProperty(MigrationConstant.PROP_SOURCE_DB_METASTORE_PORT, "");
        targetDbMetastoreHost = System.getProperty(MigrationConstant.PROP_TARGET_DB_METASTORE_HOST, "");
        targetDbMetastorePort = System.getProperty(MigrationConstant.PROP_TARGET_DB_METASTORE_PORT, "");
        sourceDbName = System.getProperty(MigrationConstant.PROP_SOURCE_DB_NAME, "");
        targetDbName = System.getProperty(MigrationConstant.PROP_TARGET_DB_NAME, "");
        tableCsvFileName = System.getProperty(MigrationConstant.PROP_TABLE_CSV_FILE_NAME, "");
        migrateTables = System.getProperty(MigrationConstant.PROP_MIGRATE_TABLES, "");
        targetDatabaseS3Dir = System.getProperty(MigrationConstant.PROP_TARGET_DATABASE_S3_DIR, "");
        dynamoDbTableName = System.getProperty(MigrationConstant.PROP_DYNAMODB_TABLE_NAME, "");
        validateSystemProperties();

        LOGGER.info(String.format(
                "Provided system properties are : %s = %s, %s = %s, %s = %s, %s = %s, %s = %s, %s = %s, %s = %s, %s = %s, %s = %s, %s = %s",
                MigrationConstant.PROP_SOURCE_DB_METASTORE_HOST, sourceDbMetastoreHost, MigrationConstant.PROP_SOURCE_DB_METASTORE_PORT,
                sourceDbMetastorePort, MigrationConstant.PROP_TARGET_DB_METASTORE_HOST, targetDbMetastoreHost,
                MigrationConstant.PROP_TARGET_DB_METASTORE_PORT, targetDbMetastorePort, MigrationConstant.PROP_SOURCE_DB_NAME, sourceDbName,
                MigrationConstant.PROP_TARGET_DB_NAME, targetDbName, MigrationConstant.PROP_TABLE_CSV_FILE_NAME, tableCsvFileName,
                MigrationConstant.PROP_MIGRATE_TABLES, migrateTables, MigrationConstant.PROP_TARGET_DATABASE_S3_DIR, targetDatabaseS3Dir,
                MigrationConstant.PROP_DYNAMODB_TABLE_NAME, dynamoDbTableName));

        hmsSourceDbClient = HiveMetastoreUtil.getHMSClient(sourceDbMetastoreHost, sourceDbMetastorePort);
        hmsTargetDbClient = HiveMetastoreUtil.getHMSClient(targetDbMetastoreHost, targetDbMetastorePort);
        fwr = FileWriterUtil.getFileWriter(tableCsvFileName);
        LOGGER.info("Initialization completed for hive schema creation process");
    }

    private void validateSystemProperties() {
        LOGGER.info("Validating system properties");
        if (sourceDbMetastoreHost.trim().isEmpty() || sourceDbMetastorePort.trim().isEmpty() || targetDbMetastoreHost.trim().isEmpty()
                || targetDbMetastorePort.trim().isEmpty() || sourceDbName.trim().isEmpty() || targetDbName.trim().isEmpty()
                || tableCsvFileName.trim().isEmpty() || targetDatabaseS3Dir.trim().isEmpty() || dynamoDbTableName.trim().isEmpty()) {

            LOGGER.error(String.format("System properties are not properly provided, please provide %s %s %s %s %s %s %s %s %s",
                    MigrationConstant.PROP_SOURCE_DB_METASTORE_HOST, MigrationConstant.PROP_SOURCE_DB_METASTORE_PORT,
                    MigrationConstant.PROP_TARGET_DB_METASTORE_HOST, MigrationConstant.PROP_TARGET_DB_METASTORE_PORT,
                    MigrationConstant.PROP_SOURCE_DB_NAME, MigrationConstant.PROP_TARGET_DB_NAME, MigrationConstant.PROP_TABLE_CSV_FILE_NAME,
                    MigrationConstant.PROP_TARGET_DATABASE_S3_DIR, MigrationConstant.PROP_DYNAMODB_TABLE_NAME));

            throw new MigrationException(String.format("System properties are not properly provided, please provide %s %s %s %s %s %s %s %s %s",
                    MigrationConstant.PROP_SOURCE_DB_METASTORE_HOST, MigrationConstant.PROP_SOURCE_DB_METASTORE_PORT,
                    MigrationConstant.PROP_TARGET_DB_METASTORE_HOST, MigrationConstant.PROP_TARGET_DB_METASTORE_PORT,
                    MigrationConstant.PROP_SOURCE_DB_NAME, MigrationConstant.PROP_TARGET_DB_NAME, MigrationConstant.PROP_TABLE_CSV_FILE_NAME,
                    MigrationConstant.PROP_TARGET_DATABASE_S3_DIR, MigrationConstant.PROP_DYNAMODB_TABLE_NAME));
        }
        LOGGER.info("Validated system properties");
    }

    private void cleanUpDynamoDb() {
        DynamoDbManager dynamoDbManager = new DynamoDbManager(hmsSourceDbClient, sourceDbName, dynamoDbTableName, migrateTables);
        List<String> hashRangeAltKeyList = dynamoDbManager
                .prepareDynamoDbKeys(targetDatabaseS3Dir.replace(MigrationConstant.TOKEN_S3_DIR_PREFIX_REMOVE, ""));
        dynamoDbManager.deleteItemsFromDynamoDb(hashRangeAltKeyList);
    }

    private void migrateDb() {
        LOGGER.info("Starting hive schema creation process");
        boolean isTargetDbPresent = HiveMetastoreUtil.createDatabase(hmsTargetDbClient, targetDbName);
        if (isTargetDbPresent) {
            List<String> tableList = HiveMetastoreUtil.getTableList(hmsSourceDbClient, sourceDbName, migrateTables);
            List<String> viewList = new ArrayList<String>();
            for (String tableStr : tableList) {
                Table table = HiveMetastoreUtil.getTableInfo(hmsSourceDbClient, sourceDbName, tableStr);
                if (table != null && HiveMetastoreUtil.isMigrateTableValid(table))
                    if (!MetaStoreUtils.isView(table))
                        writeTableInfo(table);
                    else
                        viewList.add(table.getTableName());
            }
            LOGGER.info("Completed migrating the tables");
            LOGGER.info("Starting migrating the views");
            for (String viewStr : viewList) {
                Table table = HiveMetastoreUtil.getTableInfo(hmsSourceDbClient, sourceDbName, viewStr);
                table.setDbName(targetDbName);
                HiveMetastoreUtil.createView(hmsTargetDbClient, table, sourceDbName);
            }
            LOGGER.info("Completed migrating the views");
        }
        LOGGER.info("Completed hive schema creation process");
    }

    private void writeTableInfo(Table table) {
        String sourceTableLoc = table.getSd().getLocation();
        table.setDbName(targetDbName);
        if (HiveMetastoreUtil.createTable(hmsTargetDbClient, table)) {
            StringBuffer sb = new StringBuffer();
            sb.append(table.getTableName());
            sb.append(MigrationConstant.TOKEN_COMMA);
            if (sourceTableLoc.startsWith(MigrationConstant.TOKEN_S3A)) {
                sourceTableLoc = sourceTableLoc.replaceFirst(MigrationConstant.TOKEN_S3A, MigrationConstant.TOKEN_S3);
            }
            sb.append(sourceTableLoc);
            sb.append(MigrationConstant.TOKEN_COMMA);
            if (HiveMetastoreUtil.isPartitionedTable(hmsSourceDbClient, table)) {
                sb.append(MigrationConstant.TOKEN_YES);
            } else {
                sb.append(MigrationConstant.TOKEN_NO);
            }
            StringBuilder tablePartitionNames = new StringBuilder();
            int partition_size = table.getPartitionKeysSize();
            if (partition_size >= 1) {
                for (FieldSchema f : table.getPartitionKeys()) {
                    if (partition_size > 1) {
                        tablePartitionNames.append(f.getName() + ":");
                    } else if (partition_size == 1) {
                        tablePartitionNames.append(f.getName());
                    }
                    partition_size = partition_size - 1;
                }
                sb.append(MigrationConstant.TOKEN_COMMA);
                sb.append(tablePartitionNames);
            }
            sb.append(MigrationConstant.TOKEN_NEWLINE);
            FileWriterUtil.writeToFile(fwr, sb.toString());
        }
    }
}
