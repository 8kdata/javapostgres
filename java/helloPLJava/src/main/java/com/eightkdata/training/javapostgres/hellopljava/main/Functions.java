/*
 * Copyright (c) 2014, 8Kdata Technology
 */

package com.eightkdata.training.javapostgres.hellopljava.main;

import com.eightkdata.training.javapostgres.hellopljava.dao.CountriesLanguageDAO;
import com.eightkdata.training.javapostgres.hellopljava.dao.CountriesLanguageDAO.Columns;
import com.eightkdata.training.javapostgres.hellopljava.model.CountriesLanguage;
import org.postgresql.pljava.ResultSetHandle;
import org.postgresql.pljava.annotation.Function;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * Created: 14/03/17
 * 
 * @author Matteo Melli <matteom@8kdata.com>
 */
public class Functions {
    
    public static class CountriesLanguagesHandle implements ResultSetHandle {
        
        private final int percentage;
        
        private CountriesLanguagesHandle(int percentage) {
          this.percentage = percentage;
        }
        
        @Override
        public ResultSet getResultSet() throws SQLException {
          // Connect to the database using PL/Java internal JDBC driver (no user or password needed.
          Connection connection = DriverManager.getConnection("jdbc:default:connection");
          CountriesLanguageDAO dao = new CountriesLanguageDAO(connection);
          return dao.getCountriesLanguagesResultSet(percentage);
        }
        
        @Override
        public void close() throws SQLException {
        }
    }
    
    @Function
    public static ResultSetHandle getCountriesLanguages(int percentage) throws SQLException {
        return new CountriesLanguagesHandle(percentage);
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

}
