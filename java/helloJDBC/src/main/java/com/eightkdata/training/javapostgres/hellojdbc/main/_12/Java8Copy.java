/*
 * Copyright (c) 2014, 8Kdata Technology
 */

package com.eightkdata.training.javapostgres.hellojdbc.main._12;

import com.eightkdata.training.javapostgres.hellojdbc.config.PropertiesFileDbConfig;
import com.eightkdata.training.javapostgres.hellojdbc.main._11.CountryLanguageDAO.Columns;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * @author Alvaro Hernandez <aht@8kdata.com>
 */
public class Java8Copy {
    public static void main(String[] args) {
        PropertiesFileDbConfig config;
        try {
            config = new PropertiesFileDbConfig();
        } catch (IOException e) {
            throw new RuntimeException("Error opening or reading the properties config file");
        }

        try(
                // Connect to the database. Connection is closed automagically
                Connection connection = DriverManager.getConnection(
                        config.getPostgresJdbcUrl(), config.getDbUser(), config.getDbPassword()
                )
        ) {
            // Obtain all the countriesLanguages using COPY. Note that we have hided all the driver code inside the DAO
            CountriesLanguageDAO countriesLanguageDAO = new CountriesLanguageDAO(connection);
            CountryLanguageDAO countryLanguageDAO = new CountryLanguageDAO(connection);
          
            countriesLanguageDAO.copyCountriesLanguagesTo(95, System.out);
            
            // Create a list of languages
            List<CountryLanguage> countryLanguages = Stream
                .of("Napolitan", "Sicilian", "Emilian-Romagnol")
                .map(language -> new CountryLanguage("ITA", language, false, 0.02))
                .collect(Collectors.toList());
            
            // Adding new languages using COPY.
            countryLanguageDAO.copyCountriesLanguagesFrom(countryLanguages);
            
            System.out.println();
            System.out.println(
                Columns.COUNTRY_CODE.getColumnName() + "," + Columns.LANGUAGE.getColumnName() + ","
                    + Columns.IS_OFFICIAL.getColumnName() + "," + Columns.PERCENTAGE.getColumnName()
            );
            
            // Removing added languages using a batch of deletes.
            countryLanguageDAO.getCountryLanguages("ITA")
              .forEach(cl -> System.out.println(cl.getCountryCode() + "," + cl.getLanguage() + "," + cl.isOfficial() + "," + cl.getPercentage()));
            
            // Removing added languages using a batch of deletes.
            countryLanguageDAO.deleteCountriesLanguages(countryLanguages);
        } catch (SQLException | IOException e) {
            System.err.println("Error connecting to, copying or disconnecting from the database");
            e.printStackTrace();
        }
    }
}
