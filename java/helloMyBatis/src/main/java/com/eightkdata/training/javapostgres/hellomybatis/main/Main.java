/*
 * Copyright (c) 2014, 8Kdata Technology
 */

package com.eightkdata.training.javapostgres.hellomybatis.main;

import com.eightkdata.training.javapostgres.hellomybatis.config.MybatisConfiguration;
import com.eightkdata.training.javapostgres.hellomybatis.mapper.CountriesLanguagesMapper;
import com.eightkdata.training.javapostgres.hellomybatis.model.CountriesLanguage;
import org.apache.ibatis.session.SqlSession;

import java.io.IOException;
import java.util.List;

/**
 * @author Alvaro Hernandez <aht@8kdata.com>
 */
public class Main {
    public static void main(String[] args) throws IOException {
        // Parse the configuration file mybatis-config.xml
        MybatisConfiguration config = new MybatisConfiguration();

        List<CountriesLanguage> countriesLanguages;

        // Get a session. Get the mapper (defined in countries-languages.xml) and query using business-logic style. Close session
        SqlSession sqlSession = config.getSessionFactory().openSession();
        try {
            CountriesLanguagesMapper mapper = sqlSession.getMapper(CountriesLanguagesMapper.class);
            countriesLanguages = mapper.getCountriesLanguagesByPercentage(95);
        } finally {
            if(null != sqlSession) {
                sqlSession.close();
            }
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
