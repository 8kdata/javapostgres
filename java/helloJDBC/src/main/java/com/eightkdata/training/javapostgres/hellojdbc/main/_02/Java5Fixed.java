/*
 * Copyright (c) 2014, 8Kdata Technology
 */

package com.eightkdata.training.javapostgres.hellojdbc.main._02;

import com.eightkdata.training.javapostgres.hellojdbc.config.HardcodedDbConfig;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author Alvaro Hernandez <aht@8kdata.com>
 */
public class Java5Fixed {
    public static void main(String[] args) {
        // Load postgresql driver
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            System.err.println("Driver for PostgreSQL not found. Exiting");
            System.exit(1);
        }

        try {
            // Connect to the database
            Connection connection = DriverManager.getConnection(
                    "jdbc:postgresql://" + HardcodedDbConfig.DB_HOST + ":" + HardcodedDbConfig.DB_PORT
                            + "/" + HardcodedDbConfig.DB_NAME, HardcodedDbConfig.DB_USER, HardcodedDbConfig.DB_PASSWORD
            );
            assert connection != null;      // getConnection either returns a Connection or throws an exception

            try {
                // Print some database metadata
                DatabaseMetaData metadata = connection.getMetaData();
                System.out.println(metadata.getDatabaseProductName() + " " + metadata.getDatabaseProductVersion());
            } catch (SQLException e) {
                printErrorMessageAndStackTrace("Error while querying the database", e);
            } finally {
                connection.close();     // May also throw SQLException, handled below
            }
        } catch (SQLException e) {
            printErrorMessageAndStackTrace("Error connecting to or disconnecting from the database", e);
        }
    }

    private static void printErrorMessageAndStackTrace(String error, Exception e) {
        System.err.println(error);
        e.printStackTrace();
    }
}
