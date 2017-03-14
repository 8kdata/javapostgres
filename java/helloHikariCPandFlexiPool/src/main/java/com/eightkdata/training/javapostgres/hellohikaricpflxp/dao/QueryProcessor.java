/*
 * Copyright (c) 2014, 8Kdata Technology
 */

package com.eightkdata.training.javapostgres.hellohikaricpflxp.dao;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created: 20/05/14
 *
 * @author Alvaro Hernandez <aht@8kdata.com>
 */
@FunctionalInterface
public interface QueryProcessor {
    public void process(ResultSet resultSet) throws SQLException;
}
