/*
 * Copyright (c) 2014, 8Kdata Technology
 */

package com.eightkdata.training.javapostgres.hellomybatis.mapper;

import com.eightkdata.training.javapostgres.hellomybatis.model.CountriesLanguage;

import java.util.List;

/**
 * @author Alvaro Hernandez <aht@8kdata.com>
 */
// Maps queries in countries-languages.xml
public interface CountriesLanguagesMapper {
    public List<CountriesLanguage> getCountriesLanguagesByPercentage(int percentage);
}
