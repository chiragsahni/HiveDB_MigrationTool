package com.hivedbmigration.hive.schema.migration.constant;

public interface MigrationConstant {

    String PROP_SOURCE_DB_NAME = "sourceDbName";
    String PROP_TARGET_DB_NAME = "targetDbName";
    String PROP_SOURCE_DB_METASTORE_PORT = "sourceDbMetastorePort";
    String PROP_TARGET_DB_METASTORE_PORT = "targetDbMetastorePort";
    String PROP_SOURCE_DB_METASTORE_HOST = "sourceDbMetastoreHost";
    String PROP_TARGET_DB_METASTORE_HOST = "targetDbMetastoreHost";
    String PROP_TABLE_CSV_FILE_NAME = "tableCsvFileName";
    String PROP_MIGRATE_TABLES = "migrateTables";
    String PROP_TARGET_DATABASE_S3_DIR = "targetDatabaseS3Dir";
    String PROP_DYNAMODB_TABLE_NAME = "dynamoDbTableName";
    int DEFAULT_METASTORE_CONNECTION_RETRY = 5;
    String THRIFT_URI_PREFIX = "thrift://";
    String TOKEN_COLON = ":";
    String TOKEN_COMMA = ",";
    String TOKEN_NEWLINE = "\n";
    String TOKEN_S3A = "s3a://";
    String TOKEN_S3 = "s3://";
    String TOKEN_YES = "yes";
    String TOKEN_NO = "no";
    String TOKEN_S3_DIR_PREFIX_REMOVE = "s3:/";
    String TOKEN_SLASH = "/";
    String TOKEN_HASH = "#";
    String TOKEN_SPACE = " ";
    String TOKEN_EQUALSTO = "=";
    String TOKEN_BACKTICK = "`";
    String TOKEN_DOT = ".";
    String TOKEN_HASHKEY = "hashKey";
    String TOKEN_RANGEKEY = "rangeKey";
    String TOKEN_VALUE = "value";
    String TOKEN_KEY_CONDITION_EXPR = "#hashKey = :value";
    // DYNAMO_DB_DELETION_BATCH_SIZE variable's value is double of what dynamo DB supports to delete in batch
    int DYNAMO_DB_DELETION_BATCH_SIZE = 50;

}