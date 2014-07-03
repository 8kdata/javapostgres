/*
 * Copyright (c) 2014, 8Kdata Technology
 */

package com.eightkdata.training.javapostgres.hellojdbc.main._08;

import com.eightkdata.training.javapostgres.hellojdbc.config.PropertiesFileDbConfig;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Alvaro Hernandez <aht@8kdata.com>
 */
public class Java7Model {
    private static final String QUERY = "SELECT array_agg(countrycode) AS countries, language, "
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

        List<CountriesLanguage> countriesLanguages = new ArrayList<>();
        try(
            // Connect to the database. Connection is closed automagically
            Connection connection = DriverManager.getConnection(
                    config.getPostgresJdbcUrl(), config.getDbUser(), config.getDbPassword()
            )
        ) {
            try(
                // Perform a query and extract some data. Statement is freed (closed) automagically too
                PreparedStatement statement = connection.prepareStatement(QUERY);
            ) {
                // Set the parameters and execute the query
                statement.setInt(1, 95);
                ResultSet rs = statement.executeQuery();

                // Print query results
                System.out.println("countries,language,avg_percentage");
                while(rs.next()) {
                    // Read data into a model that creates an abstraction layer. Data is stored in memory
                    countriesLanguages.add(CountriesLanguage.getInstanceFromResultSet(rs));
                }
            }
        } catch (SQLException e) {
            System.err.println("Error connecting to, querying or disconnecting from the database");
            e.printStackTrace();
        }

        // Iterate the list of countriesLanguages and print the information for each countriesLanguage
        for(CountriesLanguage cl : countriesLanguages) {
            System.out.println(cl.getCountries() + "," + cl.getLanguage() + "," + cl.getAveragePercentage());
        }
    }
}
