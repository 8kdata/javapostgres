/*
 * Copyright (c) 2014, 8Kdata Technology
 */

package com.eightkdata.training.javapostgres.hellojdbc.main._06;

import com.eightkdata.training.javapostgres.hellojdbc.config.PropertiesFileDbConfig;

import java.io.IOException;
import java.sql.*;

/**
 * @author Alvaro Hernandez <aht@8kdata.com>
 */
public class Java7QueryParametersProperties {
    private static final String QUERY = "SELECT string_agg(countrycode, ' ') AS countries, language, "
            + " to_char(avg(percentage), '999.00') AS avg_percentage"
            + " FROM countrylanguage WHERE isofficial GROUP BY language HAVING avg(percentage) > ?"
            + " ORDER BY avg(percentage) DESC";

    public static void main(String[] args) {
        PropertiesFileDbConfig config;
        try {
            config = new PropertiesFileDbConfig();
        } catch (IOException e) {
            throw new RuntimeException("Error opening or reading the properties config file");
        }

        PreparedStatement statement = null;
        try {
            // Connect to the database
            Connection connection = DriverManager.getConnection(
                    config.getPostgresJdbcUrl(), config.getDbUser(), config.getDbPassword()
            );
            assert connection != null;      // getConnection either returns a Connection or throws an exception

            try {
                // Perform a query and extract some data
                statement = connection.prepareStatement(QUERY);

                // Set the parameters and execute the query
                statement.setInt(1, 95);
                ResultSet rs = statement.executeQuery();

                // Print query results
                System.out.println("countries,language,avg_percentage");
                while(rs.next()) {
                    System.out.println(rs.getString(1) + "," + rs.getString(2) + "," + rs.getDouble(3));
                }
            } catch (SQLException e) {
                printErrorMessageAndStackTrace("Error while querying the database", e);
            } finally {
                // Free resources. Both close() methods may throw SQLException which would be handled below
                if(null != statement) {
                    statement.close();      // Also implicitly closes the ResultSet, as it was created by this statement
                }
                connection.close();
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
