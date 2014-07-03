/*
 * Copyright (c) 2014, 8Kdata Technology
 */

package com.eightkdata.training.javapostgres.hellomybatis.config;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.IOException;
import java.io.Reader;

/**
 * @author Alvaro Hernandez <aht@8kdata.com>
 */
public class MybatisConfiguration {
    private static final String MYBATIS_CONFIG_FILE = "config/mybatis-config.xml";

    private final SqlSessionFactory sqlSessionFactory;

    public MybatisConfiguration() throws IOException {
        try(Reader reader = Resources.getResourceAsReader(MYBATIS_CONFIG_FILE)) {
                sqlSessionFactory = new SqlSessionFactoryBuilder().build(reader);
        }
    }

    public SqlSessionFactory getSessionFactory() {
        return sqlSessionFactory;
    }
}
