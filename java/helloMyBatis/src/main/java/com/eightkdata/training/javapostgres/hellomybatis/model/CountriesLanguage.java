/*
 * Copyright (c) 2014, 8Kdata Technology
 */

package com.eightkdata.training.javapostgres.hellomybatis.model;

import java.text.DecimalFormat;

/**
 * Created: 30/05/14
 *
 * @author Alvaro Hernandez <aht@8kdata.com>
 */
public class CountriesLanguage {
    // Fields are non-final as they require a setter method for MyBatis
    private String countries;   // MyBatis does not support arrays
    private String language;
    private double averagePercentage;

    public void setCountries(String countries) {
        this.countries = countries;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public void setAveragePercentage(double averagePercentage) {
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
