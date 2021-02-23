package com.hivedbmigration.hive.schema.migration.exception;

public class MigrationException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private String message;

    public MigrationException(String message, Throwable cause) {
        super(message, cause);
        this.message = message;
    }
    
    public MigrationException(String message) {
        super(message);
        this.message = message;
    }

    public String toString() {
        return "MigrationException [ message = " + message + " ] ";
    }
}