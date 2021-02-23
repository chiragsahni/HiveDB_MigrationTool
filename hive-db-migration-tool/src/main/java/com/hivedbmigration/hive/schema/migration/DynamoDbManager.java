package com.hivedbmigration.hive.schema.migration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.log4j.Logger;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.hivedbmigration.hive.schema.migration.constant.MigrationConstant;
import com.hivedbmigration.hive.schema.migration.exception.MigrationException;
import com.hivedbmigration.hive.schema.migration.utils.HiveMetastoreUtil;

public class DynamoDbManager {

    /** The hms source db client. */
    private HiveMetaStoreClient hmsSourceDbClient = null;

    /** The source db name. */
    private String sourceDbName = null;

    /** The dynamo db table name. */
    private String dynamoDbTableName = null;

    /** The migrate tables. */
    private String migrateTables = null;

    /** The dynamo db. */
    private DynamoDB dynamoDb = null;

    /** The dynamo db table. */
    private Table dynamoDbTable = null;

    /** The Constant LOGGER. */
    private final static Logger LOGGER = Logger.getLogger(DynamoDbManager.class.getName());

    /** Instantiates a new dynamo db delete.
     *
     * @param hmsSourceDbClient
     *            the hms source db client
     * @param sourceDbName
     *            the source db name
     * @param dynamoDbTableName
     *            the dynamo db table name
     * @param migrateTables
     *            the migrate tables */
    public DynamoDbManager(HiveMetaStoreClient hmsSourceDbClient, String sourceDbName, String dynamoDbTableName, String migrateTables) {
        this.hmsSourceDbClient = hmsSourceDbClient;
        this.sourceDbName = sourceDbName;
        this.dynamoDbTableName = dynamoDbTableName;
        this.migrateTables = migrateTables;
        LOGGER.info("Creating DynamoDB Client");
        AmazonDynamoDB dynamoDbClient = AmazonDynamoDBClientBuilder.standard().build();
        dynamoDb = new DynamoDB(dynamoDbClient);
        LOGGER.info("Created DynamoDB Client");
        dynamoDbTable = dynamoDb.getTable(dynamoDbTableName);
        LOGGER.info("Created DynamoDB Table for table:" + dynamoDbTableName);
    }

    /** Fetch dynamo db keys.
     *
     * @param s3DatabasePath
     *            the s 3 database path
     * @return the list */
    public List<String> prepareDynamoDbKeys(String s3DatabasePath) {
        List<String> sourceTableList = HiveMetastoreUtil.getTableList(hmsSourceDbClient, sourceDbName, migrateTables);
        List<org.apache.hadoop.hive.metastore.api.Table> migrateTableList = new ArrayList<org.apache.hadoop.hive.metastore.api.Table>();
        for (String tableStr : sourceTableList) {
            org.apache.hadoop.hive.metastore.api.Table table = HiveMetastoreUtil.getTableInfo(hmsSourceDbClient, sourceDbName, tableStr);
            if (table != null && HiveMetastoreUtil.isMigrateTableValid(table))
                migrateTableList.add(table);
        }
        List<String> hashKeyList = new ArrayList<String>();
        List<String> rangeKeyList = new ArrayList<String>();
        for (org.apache.hadoop.hive.metastore.api.Table table : migrateTableList) {
            String tableName = table.getTableName();
            hashKeyList.add(s3DatabasePath);
            rangeKeyList.add(tableName);
            String tempHashKey = s3DatabasePath.concat(MigrationConstant.TOKEN_SLASH).concat(tableName);
            getItemsFromDynamoDb(dynamoDbTable, tempHashKey, hashKeyList, rangeKeyList);
        }
        List<String> hashRangeAltKeyList = new ArrayList<String>();
        if (hashKeyList.size() == rangeKeyList.size()) {
            for (int i = 0; i < hashKeyList.size(); i++) {
                hashRangeAltKeyList.add(hashKeyList.get(i));
                hashRangeAltKeyList.add(rangeKeyList.get(i));
            }
        } else {
            LOGGER.error("Processing error occurred in formation of dynamo db keys for deletion");
            throw new MigrationException("Processing error occurred in formation of dynamo db keys for deletion");
        }
        return hashRangeAltKeyList;
    }

    /** Gets the items from dynamo db.
     *
     * @param dynamoDbTable
     *            the dynamo db table
     * @param hashKey
     *            the hash key
     * @param hashKeyList
     *            the hash key list
     * @param rangeKeyList
     *            the range key list
     * @return the items from dynamo db */
    private void getItemsFromDynamoDb(Table dynamoDbTable, String hashKey, List<String> hashKeyList, List<String> rangeKeyList) {
        HashMap<String, String> nameMap = new HashMap<String, String>();
        nameMap.put(MigrationConstant.TOKEN_HASH.concat(MigrationConstant.TOKEN_HASHKEY), MigrationConstant.TOKEN_HASHKEY);
        HashMap<String, Object> valueMap = new HashMap<String, Object>();
        valueMap.put(MigrationConstant.TOKEN_COLON.concat(MigrationConstant.TOKEN_VALUE), hashKey);

        QuerySpec querySpec = new QuerySpec().withKeyConditionExpression(MigrationConstant.TOKEN_KEY_CONDITION_EXPR).withNameMap(nameMap)
                .withValueMap(valueMap);
        ItemCollection<QueryOutcome> items = null;
        Iterator<Item> iterator = null;
        Item item = null;
        try {
            items = dynamoDbTable.query(querySpec);
            iterator = items.iterator();
            while (iterator.hasNext()) {
                item = iterator.next();
                hashKeyList.add(item.getString(MigrationConstant.TOKEN_HASHKEY));
                rangeKeyList.add(item.getString(MigrationConstant.TOKEN_RANGEKEY));
                if (item.getString(MigrationConstant.TOKEN_RANGEKEY).contains(MigrationConstant.TOKEN_EQUALSTO)) {
                    String partitionHashKey = hashKey.concat(MigrationConstant.TOKEN_SLASH).concat(item.getString(MigrationConstant.TOKEN_RANGEKEY));
                    getItemsFromDynamoDb(dynamoDbTable, partitionHashKey, hashKeyList, rangeKeyList);
                }
            }
        } catch (Exception e) {
            LOGGER.error("Unable to query from dynamo DB");
            throw new MigrationException("Unable to query from dynamo DB", e);
        }
    }

    /** Delete from dynamo db.
     *
     * @param hashRangeAltKeyList
     *            the hash range alt key list */
    public void deleteItemsFromDynamoDb(List<String> hashRangeAltKeyList) {
        int listSize = hashRangeAltKeyList.size();
        LOGGER.info("Count of deletion keys from dynamo Db:" + listSize / 2);
        for (int start = 0; start < listSize; start += MigrationConstant.DYNAMO_DB_DELETION_BATCH_SIZE) {
            int end = Math.min(start + MigrationConstant.DYNAMO_DB_DELETION_BATCH_SIZE, listSize);
            List<String> deleteItemList = hashRangeAltKeyList.subList(start, end);
            makeDeleteRequest(deleteItemList);
        }
    }

    /** Make delete request.
     *
     * @param deleteItemList
     *            the delete item list */
    private void makeDeleteRequest(List<String> deleteItemList) {
        try {
            TableWriteItems deleteItems = new TableWriteItems(dynamoDbTableName).addHashAndRangePrimaryKeysToDelete(MigrationConstant.TOKEN_HASHKEY,
                    MigrationConstant.TOKEN_RANGEKEY, deleteItemList.toArray());
            LOGGER.info("Deleting dynamo db entries count:" + deleteItemList.size() / 2);
            BatchWriteItemOutcome outcome = dynamoDb.batchWriteItem(deleteItems);
            do {
                // Check for unprocessed keys which could happen if you exceed
                // provisioned throughput
                Map<String, List<WriteRequest>> unprocessedItems = outcome.getUnprocessedItems();
                if (unprocessedItems.size() != 0) {
                    LOGGER.info("Deleting dynamo db entries for unprocessed items");
                    outcome = dynamoDb.batchWriteItemUnprocessed(unprocessedItems);
                }
            } while (outcome.getUnprocessedItems().size() > 0);
            LOGGER.info("Deleted dynamo db batch entries");
        } catch (Exception e) {
            LOGGER.error("Unable to delete from dynamo DB");
            throw new MigrationException("Unable to delete from dynamo DB", e);
        }
    }
}