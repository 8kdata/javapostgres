/*
 * Copyright (c) 2014, 8Kdata Technology
 */

package com.eightkdata.training.javapostgres.hellojooq.main._03;

import static com.eightkdata.training.javapostgres.hellojooq.model.tables.Countrylanguage.COUNTRYLANGUAGE;

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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;


/**
 * Created: 10/03/17
 *
 * @author Matteo Melli <matteom@8kdata.com>
 */
public class AsynchronousChain {
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
           // Initiate an asynchronous call chain
              CompletableFuture
               
                  // This lambda will supply an int value
                  // indicating the number of inserted rows
                  .supplyAsync(() -> {
                      System.out.println("inserting language");
                      return dao
                          .insertCountryLanguage("ITA", "Napolitan", false, 0.2f)
                          .execute();
                    }
                  )
                                         
                  // This will supply a CountryLanguageRecord value
                  // for the newly inserted language
                  .handleAsync((rows, throwable) -> {
                      if (throwable == null) {
                        System.out.println("previous operation was ok");
                      } else {
                        System.out.println("previous operation was ko: " + throwable.getMessage());
                      }
                      System.out.println("reading language");
                      return dao
                          .selectCountryLanguage("ITA", "Napolitan")
                          .fetchOne();
                    }
                  )
               
                  // This should supply an int value indicating
                  // the number of rows, but in fact it'll throw
                  // a constraint violation exception
                  .handleAsync((record, throwable) -> {
                      if (throwable == null) {
                        System.out.println("previous operation was ok");
                      } else {
                        System.out.println("previous operation was ko: " + throwable.getMessage());
                      }
                      System.out.println("inserting language");
                      record.changed(true);
                      return context
                          .insertInto(COUNTRYLANGUAGE)
                          .set(record)
                          .execute();
                  })
                   
                  // This will supply an int value indicating
                  // the number of deleted rows
                  .handleAsync((rows, throwable) -> {
                      if (throwable == null) {
                        System.out.println("previous operation was ok");
                      } else {
                        System.out.println("previous operation was ko: " + throwable.getMessage());
                      }
                      System.out.println("removing language");
                      return dao
                        .deleteCountryLanguage("ITA", "Napolitan")
                        .execute();
                      
                    }
                  )
                  
                 // Check last statement
                 .handleAsync((rows, throwable) -> {
                     if (throwable == null) {
                       System.out.println("previous operation was ok");
                     } else {
                       System.out.println("previous operation was ko: " + throwable.getMessage());
                     }
                     return rows;
                   }
                 )
                  
                  // This tells the calling thread to wait for all
                  // chained execution units to be executed
                  .join();
            } catch(DataAccessException | CompletionException e) {
                System.err.println("Error executing the chain");
                e.printStackTrace();
            }
        } catch (SQLException e) {
            System.err.println("Error connecting to, executing statement or disconnecting from the database");
            e.printStackTrace();
        }
    }
}
