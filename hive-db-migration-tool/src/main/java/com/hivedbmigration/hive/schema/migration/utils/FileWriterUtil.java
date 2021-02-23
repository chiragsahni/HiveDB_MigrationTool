package com.hivedbmigration.hive.schema.migration.utils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import com.hivedbmigration.hive.schema.migration.exception.MigrationException;
import org.apache.log4j.Logger;

public class FileWriterUtil {

    private final static Logger LOGGER = Logger.getLogger(FileWriterUtil.class.getName());

    public static FileWriter getFileWriter(String fileName) {
        LOGGER.debug(String.format("Obtaining file writer for file = %s", fileName));
        try {
            File file = new File(fileName);
            if (file.exists()) {
                LOGGER.info(String.format("File = %s already exist so deleting it", fileName));
                file.delete();
                LOGGER.info(String.format("File = %s deleted", fileName));
            }
            FileWriter fwr = new FileWriter(file, true);
            LOGGER.debug(String.format("Obtained file writer for file = %s", fileName));
            return fwr;
        } catch (IOException e) {
            LOGGER.error(String.format("Error occurred in obtaining file writer for file = %s", fileName));
            throw new MigrationException("Error occurred in obtaining file writer", e);
        }
    }

    public static void writeToFile(FileWriter fwr, String content) {
        LOGGER.debug(String.format("Writing content to file = %s", content));
        try {
            fwr.append(content);
            fwr.flush();
            LOGGER.debug(String.format("Written content to file = %s", content));
        } catch (IOException e) {
            LOGGER.error(String.format("Error occurred in writing content to file = %s", content));
            throw new MigrationException("Error occurred in writing content to file", e);
        }
    }

    public static void closeFileWriter(FileWriter fwr) {
        LOGGER.debug("Closing file writer");
        try {
            if (fwr != null) {
                fwr.close();
                LOGGER.debug("Closed file writer");
            }

        } catch (IOException e) {
            LOGGER.error("Error occurred in closing file writer");
            throw new MigrationException("Error occurred in closing file writer", e);
        }
    }
}