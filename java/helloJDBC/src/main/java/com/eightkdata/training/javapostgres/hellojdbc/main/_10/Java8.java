/*
 * Copyright (c) 2014, 8Kdata Technology
 */

package com.eightkdata.training.javapostgres.hellojdbc.main._10;

import com.eightkdata.training.javapostgres.hellojdbc.config.PropertiesFileDbConfig;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;


/**
 * @author Alvaro Hernandez <aht@8kdata.com>
 */
public class Java8 {
    public static void main(String[] args) {
        PropertiesFileDbConfig config;
        try {
            config = new PropertiesFileDbConfig();
        } catch (IOException e) {
            throw new RuntimeException("Error opening or reading the properties config file");
        }

        List<CountriesLanguage> countriesLanguages = null;
        try(
                // Connect to the database. Connection is closed automagically
                Connection connection = DriverManager.getConnection(
                        config.getPostgresJdbcUrl(), config.getDbUser(), config.getDbPassword()
                )
        ) {
            // Obtain all the countriesLanguages. Note that we have hided all the JDBC code inside the DAO
            CountriesLanguageDAO countriesLanguageDAO = new CountriesLanguageDAO(connection);
            countriesLanguages = countriesLanguageDAO.getCountriesLanguages(95);
        } catch (SQLException e) {
            System.err.println("Error connecting to, querying or disconnecting from the database");
            e.printStackTrace();
        }

        if(countriesLanguages != null) {
            // Print query results
            countriesLanguages.forEach(
                    r -> System.out.println(r.getCountries() + "," + r.getLanguage() + "," + r.getAveragePercentage())
            );
        }
    }
}
