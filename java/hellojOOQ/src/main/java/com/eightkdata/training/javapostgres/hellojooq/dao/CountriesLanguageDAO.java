/*
 * Copyright (c) 2014, 8Kdata Technology
 */

package com.eightkdata.training.javapostgres.hellojooq.dao;

import com.eightkdata.training.javapostgres.hellojooq.model.CountriesLanguage;
import org.jooq.DSLContext;
import org.jooq.Record3;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import java.math.BigDecimal;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static com.eightkdata.training.javapostgres.hellojooq.model.tables.Countrylanguage.COUNTRYLANGUAGE;
import static org.jooq.impl.DSL.avg;
import static org.jooq.impl.DSL.listAgg;

/**
 * @author Alvaro Hernandez <aht@8kdata.com>
 */
public class CountriesLanguageDAO {
    private final DSLContext context;

    public CountriesLanguageDAO(Connection connection) {
        context = DSL.using(connection, SQLDialect.POSTGRES);
    }

    private CountriesLanguage getInstanceFromRecord(Record3<String, String, BigDecimal> record) {
        return new CountriesLanguage(
                (String) record.getValue("countries"),      // Get field by String ("as" name)
                record.getValue(COUNTRYLANGUAGE.LANGUAGE),  // Get field in type-safe manner
                record.value3().doubleValue()               // Get field by position in the query
        );
    }

    public List<CountriesLanguage> getCountriesLanguages(int percentage) throws DataAccessException {
        Result<Record3<String,String,BigDecimal>> result = context
                // jOOQ helps us generate an advanced, yet type-safe query
                .select(
                        listAgg(COUNTRYLANGUAGE.COUNTRYCODE, ",")
                                .withinGroupOrderBy(COUNTRYLANGUAGE.COUNTRYCODE).as("countries"),
                        COUNTRYLANGUAGE.LANGUAGE,
                        avg(COUNTRYLANGUAGE.PERCENTAGE).as("avgPercentage")
                )
                .from(COUNTRYLANGUAGE)
                .where(COUNTRYLANGUAGE.ISOFFICIAL)
                .groupBy(COUNTRYLANGUAGE.LANGUAGE)
                .having(avg(COUNTRYLANGUAGE.PERCENTAGE).greaterOrEqual(BigDecimal.valueOf(percentage)))
                .orderBy(avg(COUNTRYLANGUAGE.PERCENTAGE).desc())
                // end of query
        .fetch();

        List<CountriesLanguage> countriesLanguages = new ArrayList<>(result.size());
        for(Record3<String,String,BigDecimal> record : result) {
            countriesLanguages.add(getInstanceFromRecord(record));
        }

        return countriesLanguages;
    }
}
