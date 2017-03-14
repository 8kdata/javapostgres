/*
 * Copyright (c) 2014, 8Kdata Technology
 */

package com.eightkdata.training.javapostgres.hellojooq.main._02;

import com.eightkdata.training.javapostgres.hellojooq.config.PropertiesFileDbConfig;
import com.eightkdata.training.javapostgres.hellojooq.dao.CountriesLanguageDAO;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


/**
 * Created: 10/03/17
 *
 * @author Matteo Melli <matteom@8kdata.com>
 */
public class ResultSetStream {
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
            DSLContext context = DSL.using(connection, SQLDialect.POSTGRES);
            CountriesLanguageDAO dao = new CountriesLanguageDAO(context);
            try {
              int percentage = 95;
              System.out.println("countries,language,avg_percentage");
              dao
                // Retrieve the select object
                .selectCountriesLanguages(percentage)
                    
                // Now stream the results... Yeah, piece of cake!
                .stream()
    
                // We can use lambda expressions to map jOOQ Records
                .map(record -> dao.toCountriesLanguage(record))
  
                // ... and then profit from the new Collection methods
                .forEach(cl -> System.out.println(cl.getCountries() + "," + cl.getLanguage() + "," + cl.getAveragePercentage()));
            } catch(DataAccessException e) {
                System.err.println("Error executing the query");
                e.printStackTrace();
            }
        } catch (SQLException e) {
            System.err.println("Error connecting to, querying or disconnecting from the database");
            e.printStackTrace();
        }
    }
}
