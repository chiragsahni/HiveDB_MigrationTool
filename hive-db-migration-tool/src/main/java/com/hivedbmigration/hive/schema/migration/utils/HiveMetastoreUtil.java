package com.hivedbmigration.hive.schema.migration.utils;

import java.util.Arrays;
import java.util.List;

import com.hivedbmigration.hive.schema.migration.constant.MigrationConstant;
import com.hivedbmigration.hive.schema.migration.exception.MigrationException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

public class HiveMetastoreUtil {

    private final static Logger LOGGER = Logger.getLogger(HiveMetastoreUtil.class.getName());

    public static HiveMetaStoreClient getHMSClient(String host, String port) {
        LOGGER.info(String.format("Obtaining hive metastore client for host = %s and port = %s", host, port));
        HiveConf hiveConf = new HiveConf();
        hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, MigrationConstant.DEFAULT_METASTORE_CONNECTION_RETRY);
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, MigrationConstant.THRIFT_URI_PREFIX + host + MigrationConstant.TOKEN_COLON + port);
        try {
            HiveMetaStoreClient hmsClient = new HiveMetaStoreClient(hiveConf);
            LOGGER.info(String.format("Obtained hive metastore client for host = %s and port = %s", host, port));
            return hmsClient;
        } catch (MetaException e) {
            LOGGER.error(String.format("Error occurred in obtaining hive metastore client for host = %s and port = %s", host, port));
            throw new MigrationException("Error occurred in obtaining hive metastore client", e);
        }
    }

    public static List<String> getTableList(HiveMetaStoreClient hmsClient, String dbName, String migrateTables) {
        List<String> tableList = null;
        if (!migrateTables.isEmpty()) {
            LOGGER.info("Migrating tables present in database:" + migrateTables);
            tableList = Arrays.asList(migrateTables.split(MigrationConstant.TOKEN_COMMA));
        } else {
            LOGGER.info(String.format("Obtaining table list present in database = %s", dbName));
            try {
                tableList = hmsClient.getAllTables(dbName);
                LOGGER.info(String.format("Obtained table list present in database = %s tables = %s", dbName, tableList));
            } catch (MetaException e) {
                LOGGER.error(String.format("Error occurred in obtaining table list of database = %s", dbName));
                throw new MigrationException("Error occurred in obtaining table list", e);
            }
        }
        return tableList;
    }

    public static Table getTableInfo(HiveMetaStoreClient hmsClient, String dbName, String tableName) {
        LOGGER.info(String.format("Obtaining table info for database = %s table = %s", dbName, tableName));
        try {
            Table table = hmsClient.getTable(dbName, tableName);
            LOGGER.info(String.format("Obtained table info for database = %s table = %s", dbName, tableName));
            return table;
        } catch (NoSuchObjectException e) {
            LOGGER.error(String.format("Table = %s does not exist in database = %s", tableName, dbName));
            return null;
        } catch (TException e) {
            LOGGER.error(String.format("Error occurred in obtaining table info for database = %s; table = %s", dbName, tableName));
            throw new MigrationException("Error occurred in obtaining table information", e);
        }
    }

    public static boolean createDatabase(HiveMetaStoreClient hmsClient, String dbName) {
        LOGGER.info(String.format("Creating database = %s", dbName));
        Database db = new Database();
        db.setName(dbName);
        try {
            hmsClient.createDatabase(db);
            LOGGER.info(String.format("Created database = %s", dbName));
            return true;
        } catch (AlreadyExistsException e) {
            LOGGER.info(String.format("Already exist database = %s", dbName));
            return true;
        } catch (TException e) {
            LOGGER.error(String.format("Error occurred in creating database = %s", dbName));
            throw new MigrationException("Error occurred in creating database", e);
        }
    }

    public static boolean createTable(HiveMetaStoreClient hmsClient, Table table) {
        LOGGER.info(String.format("Creating table = %s in database = %s", table.getTableName(), table.getDbName()));
        try {
            table.getSd().setLocation("");
            table.getSd().setLocationIsSet(false);
            hmsClient.createTable(table);
            LOGGER.info(String.format("Created table = %s in database = %s", table.getTableName(), table.getDbName()));
            return true;
        } catch (AlreadyExistsException e) {
            LOGGER.info(String.format("Already exist table = %s in database = %s", table.getTableName(), table.getDbName()));
            return false;
        } catch (TException e) {
            LOGGER.info(String.format("Error occurred in creating table = %s in database = %s", table.getTableName(), table.getDbName()));
            throw new MigrationException("Error occurred in creating table", e);
        }
    }

    public static void createView(HiveMetaStoreClient hmsClient, Table table, String sourceDbName) {
        LOGGER.info(String.format("Creating View = %s in database = %s", table.getTableName(), table.getDbName()));
        try {
            String viewDefinition = table.getViewExpandedText();
            viewDefinition = viewDefinition.replaceAll(
                    MigrationConstant.TOKEN_BACKTICK + sourceDbName + MigrationConstant.TOKEN_BACKTICK + MigrationConstant.TOKEN_DOT,
                    MigrationConstant.TOKEN_BACKTICK + table.getDbName() + MigrationConstant.TOKEN_BACKTICK + MigrationConstant.TOKEN_DOT);
            table.setViewExpandedText(viewDefinition);
            table.setTableType(TableType.VIRTUAL_VIEW.name());
            hmsClient.createTable(table);
            LOGGER.info(String.format("Created view = %s in database = %s", table.getTableName(), table.getDbName()));
        } catch (AlreadyExistsException e) {
            LOGGER.info(String.format("Already exist view = %s in database = %s", table.getTableName(), table.getDbName()));
        } catch (TException e) {
            LOGGER.info(String.format("Error occurred in creating view = %s in database = %s", table.getTableName(), table.getDbName()));
            throw new MigrationException("Error occurred in creating view", e);
        }
    }

    public static boolean isPartitionedTable(HiveMetaStoreClient hmsClient, Table table) {
        LOGGER.info(String.format("Checking table = %s is partitioned or not", table.getTableName()));
        if (table.getPartitionKeysSize() != 0) {
            LOGGER.info(String.format("Table = %s is partitioned", table.getTableName()));
            return true;
        } else {
            LOGGER.info(String.format("Table = %s is not partitioned", table.getTableName()));
            return false;
        }
    }

    public static void closeHMSClient(HiveMetaStoreClient hmsClient) {
        LOGGER.info("Closing HiveMetaStoreClient");
        if (hmsClient != null) {
            hmsClient.close();
            LOGGER.info("Closed HiveMetaStoreClient");
        }
    }

    public static boolean isMigrateTableValid(Table table) {
        if (!table.getTableType().equalsIgnoreCase("EXTERNAL_TABLE")) {
            LOGGER.info("Considering table for migration process:" + table.getTableName());
            return true;
        }
        LOGGER.info("Skipping table for migration process:" + table.getTableName());
        return false;
    }
}