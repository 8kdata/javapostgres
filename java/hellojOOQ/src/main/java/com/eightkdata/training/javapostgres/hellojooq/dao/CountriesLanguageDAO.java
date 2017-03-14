/*
 * Copyright (c) 2014, 8Kdata Technology
 */

package com.eightkdata.training.javapostgres.hellojooq.dao;

import static com.eightkdata.training.javapostgres.hellojooq.model.tables.Countrylanguage.COUNTRYLANGUAGE;
import static org.jooq.impl.DSL.avg;
import static org.jooq.impl.DSL.listAgg;

import com.eightkdata.training.javapostgres.hellojooq.model.CountriesLanguage;
import com.eightkdata.training.javapostgres.hellojooq.model.tables.records.CountrylanguageRecord;
import org.jooq.DSLContext;
import org.jooq.Delete;
import org.jooq.Insert;
import org.jooq.Record;
import org.jooq.Record3;
import org.jooq.Select;
import org.jooq.exception.DataAccessException;

import java.math.BigDecimal;

/**
 * @author Alvaro Hernandez <aht@8kdata.com>
 * @author Matteo Melli <matteom@8kdata.com>
 */
public class CountriesLanguageDAO {
    private final DSLContext context;

    public CountriesLanguageDAO(DSLContext context) {
        this.context = context;
    }

    public Select<Record3<String,String,BigDecimal>> selectCountriesLanguages(int percentage) throws DataAccessException {
        return context
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
        ;
    }

    public Select<Record> selectCountryLanguage(String countryCode, String language) {
      return context
          .select().from(COUNTRYLANGUAGE).where(COUNTRYLANGUAGE.COUNTRYCODE.eq(countryCode)
              .and(COUNTRYLANGUAGE.LANGUAGE.eq(language)));
    }

    public Insert<CountrylanguageRecord> insertCountryLanguage(String countryCode, String language, boolean isOfficial, float percentage) {
      return context
        .insertInto(COUNTRYLANGUAGE)
        .values(countryCode, language, isOfficial, percentage);
    }

    public Delete<CountrylanguageRecord> deleteCountryLanguage(String countryCode, String language) {
      return context
          .delete(COUNTRYLANGUAGE)
          .where(COUNTRYLANGUAGE.COUNTRYCODE.eq(countryCode)
              .and(COUNTRYLANGUAGE.LANGUAGE.eq(language)));
    }

    public CountriesLanguage toCountriesLanguage(Record3<String, String, BigDecimal> record) {
        return new CountriesLanguage(
                (String) record.getValue("countries"),      // Get field by String ("as" name)
                record.getValue(COUNTRYLANGUAGE.LANGUAGE),  // Get field in type-safe manner
                record.value3().doubleValue()               // Get field by position in the query
        );
    }
}
