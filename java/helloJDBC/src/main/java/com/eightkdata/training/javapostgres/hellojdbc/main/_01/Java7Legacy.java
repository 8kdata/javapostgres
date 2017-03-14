/*
 * Copyright (c) 2014, 8Kdata Technology
 */

package com.eightkdata.training.javapostgres.hellojdbc.main._01;

import com.eightkdata.training.javapostgres.hellojdbc.config.HardcodedDbConfig;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author Alvaro Hernandez <aht@8kdata.com>
 */
public class Java7Legacy {
    // Do not use this code as an example. It has some errors fixed in following examples

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        // Load postgresql driver
        Class.forName("org.postgresql.Driver");

        // Connect to the database
        Connection connection = DriverManager.getConnection(
                "jdbc:postgresql://" + HardcodedDbConfig.DB_HOST + ":" + HardcodedDbConfig.DB_PORT
                        + "/" + HardcodedDbConfig.DB_NAME, HardcodedDbConfig.DB_USER, HardcodedDbConfig.DB_PASSWORD
        );

        // Print some database metadata
        DatabaseMetaData metadata = connection.getMetaData();
        System.out.println(metadata.getDatabaseProductName() + " " + metadata.getDatabaseProductVersion());
    }
}
