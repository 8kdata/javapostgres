/*
 * Copyright (c) 2014, 8Kdata Technology
 */

package com.eightkdata.training.javapostgres.hellojooq.model;

import java.text.DecimalFormat;

/**
 * Created: 30/05/14
 *
 * @author Alvaro Hernandez <aht@8kdata.com>
 */
public class CountriesLanguage {
    private final String countries;
    private final String language;
    private final double averagePercentage;

    public CountriesLanguage(String countries, String language, double averagePercentage) {
        this.countries = countries;
        this.language = language;
        this.averagePercentage = averagePercentage;
    }

    public String getCountries() {
        return "[" + countries + "]";
    }

    public String getLanguage() {
        return language;
    }

    public String getAveragePercentage() {
        return new DecimalFormat("#.00").format(averagePercentage);
    }
}
