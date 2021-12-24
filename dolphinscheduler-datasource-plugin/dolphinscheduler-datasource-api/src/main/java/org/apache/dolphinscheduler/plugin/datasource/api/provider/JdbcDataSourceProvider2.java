/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.plugin.datasource.api.provider;

import org.apache.dolphinscheduler.plugin.datasource.api.exception.DataSourceException;
import org.apache.dolphinscheduler.spi.datasource.JdbcConnectionParam;
import org.apache.dolphinscheduler.spi.enums.DbType;
import org.apache.dolphinscheduler.spi.utils.Constants;
import org.apache.dolphinscheduler.spi.utils.PropertyUtils;
import org.apache.dolphinscheduler.spi.utils.StringUtils;

import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.Driver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * provider create Jdbc Data Source
 */
public class JdbcDataSourceProvider2 {

    private static final Logger logger = LoggerFactory.getLogger(JdbcDataSourceProvider2.class);

    public static BasicDataSource createJdbcDataSource(JdbcConnectionParam connectionParam) {
        logger.info("Creating DruidDataSource pool for maxActive:{}", PropertyUtils.getInt(Constants.SPRING_DATASOURCE_MAX_ACTIVE, 50));
        BasicDataSource dataSource = new BasicDataSource();

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Driver driver = getDataSourceDriver(connectionParam, classLoader);

        dataSource.setDriverClassLoader(classLoader);
        if (driver != null) {
            dataSource.setDriver(driver);
        }
        dataSource.setDriverClassName(connectionParam.getDriverClassName());
        dataSource.setUrl(connectionParam.getJdbcUrl());
        dataSource.setUsername(connectionParam.getUser());
        dataSource.setPassword(connectionParam.getPassword());

        dataSource.setMaxWaitMillis(PropertyUtils.getInt(Constants.SPRING_DATASOURCE_MAX_ACTIVE, 50));
        dataSource.setMaxTotal(PropertyUtils.getInt(Constants.SPRING_DATASOURCE_MAX_ACTIVE, 50));
        dataSource.setMinIdle(PropertyUtils.getInt(Constants.SPRING_DATASOURCE_MIN_IDLE, 5));
        dataSource.setMaxIdle(PropertyUtils.getInt(Constants.SPRING_DATASOURCE_MIN_IDLE, 5));
//        dataSource.setMaxConnLifetimeMillis(poolConfig.getMaxConnLifetimeMillis());
//        dataSource.setTimeBetweenEvictionRunsMillis(poolConfig.getTimeBetweenEvictionRunsMillis());
//        dataSource.setMinEvictableIdleTimeMillis(poolConfig.getMinEvictableIdleTimeMillis());
//        dataSource.setSoftMinEvictableIdleTimeMillis(poolConfig.getSoftMinEvictableIdleTimeMillis());

        dataSource.setValidationQuery("select 1");
        dataSource.setTestOnBorrow(true);

//        basicDataSource.setDriverClassLoader();
//        druidDataSource.setDriverClassName(connectionParam.getDriverClassName());
//        druidDataSource.setUrl(connectionParam.getJdbcUrl());
//        druidDataSource.setUsername(connectionParam.getUser());
//        druidDataSource.setPassword(PasswordUtils.decodePassword(connectionParam.getPassword()));
//
//        druidDataSource.setMinIdle();
//        druidDataSource.setMaxActive();
//        druidDataSource.setTestOnBorrow(PropertyUtils.getBoolean(Constants.SPRING_DATASOURCE_TEST_ON_BORROW, false));

        if (connectionParam.getProps() != null) {
            //connectionParam.getProps().forEach(dataSource::addConnectionProperty);
        }

        logger.info("Creating HikariDataSource pool success.");
        return dataSource;
    }

    /**
     * @return One Session Jdbc DataSource
     */
    public static BasicDataSource createOneSessionJdbcDataSource(JdbcConnectionParam connectionParam) {
        logger.info("Creating DruidDataSource pool for maxActive:{}", PropertyUtils.getInt(Constants.SPRING_DATASOURCE_MAX_ACTIVE, 50));
        BasicDataSource dataSource = new BasicDataSource();

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Driver driver = getDataSourceDriver(connectionParam, classLoader);

        dataSource.setDriverClassLoader(classLoader);
        if (driver != null) {
            dataSource.setDriver(driver);
        }
        dataSource.setDriverClassName(connectionParam.getDriverClassName());
        dataSource.setUrl(connectionParam.getJdbcUrl());
        dataSource.setUsername(connectionParam.getUser());
        dataSource.setPassword(connectionParam.getPassword());

        dataSource.setMaxTotal(1);
        dataSource.setMinIdle(1);
        dataSource.setMaxIdle(1);
        dataSource.setValidationQuery("select 1");
        dataSource.setTestOnBorrow(true);
        if (connectionParam.getProps() != null) {
            //connectionParam.getProps().forEach(dataSource::addConnectionProperty);
        }
        logger.info("Creating OneSession HikariDataSource pool success.");
        return dataSource;
    }

    protected static void loaderJdbcDriver(ClassLoader classLoader, JdbcConnectionParam connectionParam) {
        String drv = StringUtils.isBlank(connectionParam.getDriverClassName()) ? connectionParam.getDbType().getDefaultDriverClass() : connectionParam.getDriverClassName();
        try {
            final Class<?> clazz = Class.forName(drv, true, classLoader);
            final Driver driver = (Driver) clazz.newInstance();
            if (!driver.acceptsURL(connectionParam.getJdbcUrl())) {
                logger.warn("Jdbc driver loading error. Driver {} cannot accept url.", drv);
                throw new RuntimeException("Jdbc driver loading error.");
            }
            if (connectionParam.getDbType().equals(DbType.MYSQL)) {
                if (driver.getMajorVersion() >= 8) {
                    connectionParam.setDriverClassName(drv);
                } else {
                    connectionParam.setDriverClassName(Constants.COM_MYSQL_JDBC_DRIVER);
                }
            }
        } catch (final Exception e) {
            logger.warn("The specified driver not suitable.");
        }
    }

    private static Driver getDataSourceDriver(JdbcConnectionParam connectionParam, ClassLoader classLoader) {
        Driver driver;
        try {
            final Class<?> clazz = Class.forName(connectionParam.getDriverClassName(), true, classLoader);
            driver = (Driver) clazz.newInstance();
        } catch (Exception e) {
            throw DataSourceException.getInstance("Jdbc driver init error.", e);
        }
        return driver;
    }
}
