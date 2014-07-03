/*
 * Copyright (c) 2014, 8Kdata Technology
 */

package com.eightkdata.training.javapostgres.hellojooq.main;

import com.eightkdata.training.javapostgres.hellojooq.model.CountriesLanguage;
import com.eightkdata.training.javapostgres.hellojooq.dao.CountriesLanguageDAO;
import com.eightkdata.training.javapostgres.hellojooq.config.PropertiesFileDbConfig;
import org.jooq.exception.DataAccessException;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;


/**
 * Created: 18/05/14
 *
 * @author Alvaro Hernandez <aht@8kdata.com>
 */
public class Main {
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
            try {
                CountriesLanguageDAO countriesLanguageDAO = new CountriesLanguageDAO(connection);
                countriesLanguages = countriesLanguageDAO.getCountriesLanguages(95);
            } catch(DataAccessException e) {
                System.err.println("Error executing the query");
                e.printStackTrace();
            }
        } catch (SQLException e) {
            System.err.println("Error connecting to, querying or disconnecting from the database");
            e.printStackTrace();
        }

        if(countriesLanguages != null) {
            // Print query results
            System.out.println("countries,language,avg_percentage");
            for(CountriesLanguage cl : countriesLanguages) {
                System.out.println(cl.getCountries() + "," + cl.getLanguage() + "," + cl.getAveragePercentage());
            }
        }
    }
}
