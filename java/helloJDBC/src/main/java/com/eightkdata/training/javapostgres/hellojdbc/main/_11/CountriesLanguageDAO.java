/*
 * Copyright (c) 2014, 8Kdata Technology
 */

package com.eightkdata.training.javapostgres.hellojdbc.main._11;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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

    private static final String COUNTRIES_LANGUAGES_QUERY = "SELECT array_agg(countrycode) AS " + Columns.COUNTRIES.columnName
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
                (String[]) rs.getArray(1).getArray(), rs.getString(2), rs.getDouble(3)
        );
    }

    public List<CountriesLanguage> getCountriesLanguages(int percentage) throws SQLException {
        try(
            // Perform a query and extract some data. Statement is freed (closed) automagically too
            PreparedStatement statement = connection.prepareStatement(COUNTRIES_LANGUAGES_QUERY)
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

}
