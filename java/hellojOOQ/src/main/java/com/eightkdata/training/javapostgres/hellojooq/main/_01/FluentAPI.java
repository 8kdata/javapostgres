/*
 * Copyright (c) 2014, 8Kdata Technology
 */

package com.eightkdata.training.javapostgres.hellojooq.main._01;

import com.eightkdata.training.javapostgres.hellojooq.config.PropertiesFileDbConfig;
import com.eightkdata.training.javapostgres.hellojooq.dao.CountriesLanguageDAO;
import com.eightkdata.training.javapostgres.hellojooq.model.CountriesLanguage;
import org.jooq.DSLContext;
import org.jooq.Record3;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


/**
 * Created: 18/05/14
 *
 * @author Alvaro Hernandez <aht@8kdata.com>
 * 
 * Updated: 10/03/17
 * 
 * @author Matteo Melli <matteom@8kdata.com>
 */
public class FluentAPI {
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
            DSLContext context = DSL.using(connection, SQLDialect.POSTGRES);
            CountriesLanguageDAO dao = new CountriesLanguageDAO(context);
            try {
              int percentage = 95;
              Result<Record3<String,String,BigDecimal>> result = dao
                  // Retrieve the select object
                  .selectCountriesLanguages(percentage)
                  
                  // And fetch the result
                  .fetch();
              
              countriesLanguages = new ArrayList<>(result.size());
              for(Record3<String,String,BigDecimal> record : result) {
                  countriesLanguages.add(dao.toCountriesLanguage(record));
              }
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
