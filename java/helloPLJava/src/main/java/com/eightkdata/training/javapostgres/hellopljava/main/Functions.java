/*
 * Copyright (c) 2014, 8Kdata Technology
 */

package com.eightkdata.training.javapostgres.hellopljava.main;

import com.eightkdata.training.javapostgres.hellopljava.dao.CountriesLanguageDAO;
import com.eightkdata.training.javapostgres.hellopljava.dao.CountriesLanguageDAO.Columns;
import com.eightkdata.training.javapostgres.hellopljava.dao.CountryLanguageDAO;
import com.eightkdata.training.javapostgres.hellopljava.model.CountriesLanguage;
import org.postgresql.pljava.ResultSetHandle;
import org.postgresql.pljava.ResultSetProvider;
import org.postgresql.pljava.annotation.Function;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Created: 14/03/17
 * 
 * @author Matteo Melli <matteom@8kdata.com>
 */
public class Functions {
  
  @Function
  public static ResultSetProvider getCountriesLanguages(int percentage) throws SQLException {
      return new CountriesLanguagesHandle(percentage);
  }
  
  public static class CountriesLanguagesHandle implements ResultSetProvider {
      
      private CountriesLanguageDAO dao;
      private PreparedStatement statement;
      private ResultSet resultSet;
      
      private CountriesLanguagesHandle(int percentage) throws SQLException {
        // Connect to the database using PL/Java internal JDBC driver (no user or password needed.
        Connection connection = DriverManager.getConnection("jdbc:default:connection");
        dao = new CountriesLanguageDAO(connection);
        statement = dao.getCountriesLanguagesStatement(percentage);
        resultSet = statement.executeQuery();
      }
      
      @Override
      public boolean assignRowValues(ResultSet receiver, int currentRow) throws SQLException {
        return dao.assignRowValues(resultSet, receiver);
      }
      
      @Override
      public void close() throws SQLException {
        statement.close();
      }
  }
  
  @Function(type="countrylanguage")
  public static ResultSetHandle getCountryLanguages(String countryCode) throws SQLException {
      return new CountryLanguagesHandle(countryCode);
  }
  
  public static class CountryLanguagesHandle implements ResultSetHandle {
      
      private final String countryCode;
      
      private PreparedStatement statement;
      
      private CountryLanguagesHandle(String countryCode) {
        this.countryCode = countryCode;
      }
      
      @Override
      public ResultSet getResultSet() throws SQLException {
        // Connect to the database using PL/Java internal JDBC driver (no user or password needed.
        Connection connection = DriverManager.getConnection("jdbc:default:connection");
        CountryLanguageDAO dao = new CountryLanguageDAO(connection);
        statement = dao.getCountryLanguagesStatement(countryCode);
        return statement.executeQuery();
      }
      
      @Override
      public void close() throws SQLException {
        statement.close();
      }
  }
    
    @Function
    public static Iterator<String> getCountriesLanguagesCSV(int percentage) throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:default:connection");
        CountriesLanguageDAO dao = new CountriesLanguageDAO(connection);
        List<CountriesLanguage> countriesLanguages = dao.getCountriesLanguages(percentage);
        List<String> result = new ArrayList<>();
        result.add(
            Columns.COUNTRIES.getColumnName() + "," + Columns.LANGUAGE.getColumnName() + ","
                  + Columns.AVG_PERCENTAGE.getColumnName()
        );
        countriesLanguages
            .forEach(cl -> result.add(cl.getCountries() + "," + cl.getLanguage() + "," + cl.getAveragePercentage()));
        return result.iterator();
    }

    @Function
    public static void log(String level, String msg) {
      Logger.getAnonymousLogger()
        .log(Level.parse(level), msg);
    }
}
