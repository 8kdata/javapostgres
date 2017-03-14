/*
 * Copyright (c) 2014, 8Kdata Technology
 */

package com.eightkdata.training.javapostgres.hellopljava.dao;



import com.eightkdata.training.javapostgres.hellopljava.model.CountriesLanguage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Alvaro Hernandez <aht@8kdata.com>
 */
public class CountriesLanguageDAO {
    public enum Columns {
        COUNTRIES("countries"),
        LANGUAGE("language"),
        AVG_PERCENTAGE("avg_percentage");

        private final String columnName;

        private Columns(String columnName) {
            this.columnName = columnName;
        }

        public String getColumnName() {
            return columnName;
        }
    }

    private static final String QUERY = "SELECT array_agg(countrycode) AS " + Columns.COUNTRIES.columnName
            + ", " + Columns.LANGUAGE.columnName + ", to_char(avg(percentage), '999.00') AS "
            + Columns.AVG_PERCENTAGE.columnName
            + " FROM countrylanguage WHERE isofficial GROUP BY language HAVING avg(percentage) > ?"
            + " ORDER BY avg(percentage) DESC";

    private final Connection connection;

    public CountriesLanguageDAO(Connection connection) {
        this.connection = connection;
    }

    private CountriesLanguage getInstanceFromResultSet(ResultSet rs) throws SQLException {
        return new CountriesLanguage(
                (String[]) rs.getObject(1), rs.getString(2), rs.getDouble(3)
        );
    }
    
    public boolean assignRowValues(ResultSet resultSet, ResultSet receiver) throws SQLException {
      if (resultSet.next()) {
          receiver.updateObject(1, resultSet.getObject(1));
          receiver.updateString(2, resultSet.getString(2));
          receiver.updateDouble(3, resultSet.getDouble(3));
          
          return true;
      }
      
      return false;
    }

    public List<CountriesLanguage> getCountriesLanguages(int percentage) throws SQLException {
        try(
            // Perform a query and extract some data. Statement is freed (closed) automagically too
            PreparedStatement statement = connection.prepareStatement(QUERY)
        ) {
            // Set the parameters and execute the query
            statement.setInt(1, percentage);

            List<CountriesLanguage> countriesLanguages = new ArrayList<>();
            QueryExecutor.executeQuery(statement, (rs) -> {
                countriesLanguages.add(getInstanceFromResultSet(rs));
            });

            return countriesLanguages;
        }
    }

    public PreparedStatement getCountriesLanguagesStatement(int percentage) throws SQLException {
        // Perform a query and return the result. Statement and ResultSet are not freed (closed)
        PreparedStatement statement = connection.prepareStatement(QUERY);
        // Set the parameters and return statement
        statement.setInt(1, percentage);
        return statement;
    }
}
