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

import org.apache.dolphinscheduler.plugin.datasource.api.utils.PasswordUtils;
import org.apache.dolphinscheduler.spi.datasource.JdbcConnectionParam;
import org.apache.dolphinscheduler.spi.enums.DbType;
import org.apache.dolphinscheduler.spi.utils.Constants;
import org.apache.dolphinscheduler.spi.utils.PropertyUtils;
import org.apache.dolphinscheduler.spi.utils.StringUtils;

import java.sql.Driver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.pool.DruidDataSource;

/**
 * provider create Jdbc Data Source
 */
public class JdbcDataSourceProvider2 {

    private static final Logger logger = LoggerFactory.getLogger(JdbcDataSourceProvider2.class);

    public static DruidDataSource createJdbcDataSource(JdbcConnectionParam connectionParam) {
        logger.info("Creating DruidDataSource pool for maxActive:{}", PropertyUtils.getInt(Constants.SPRING_DATASOURCE_MAX_ACTIVE, 50));

        DruidDataSource druidDataSource = new DruidDataSource();

        druidDataSource.setDriverClassName(connectionParam.getDriverClassName());
        druidDataSource.setUrl(connectionParam.getJdbcUrl());
        druidDataSource.setUsername(connectionParam.getUser());
        druidDataSource.setPassword(PasswordUtils.decodePassword(connectionParam.getPassword()));

        druidDataSource.setMinIdle(PropertyUtils.getInt(Constants.SPRING_DATASOURCE_MIN_IDLE, 5));
        druidDataSource.setMaxActive(PropertyUtils.getInt(Constants.SPRING_DATASOURCE_MAX_ACTIVE, 50));
        druidDataSource.setTestOnBorrow(PropertyUtils.getBoolean(Constants.SPRING_DATASOURCE_TEST_ON_BORROW, false));

        if (connectionParam.getProps() != null) {
            druidDataSource.setConnectProperties(connectionParam.getProps());
        }

        logger.info("Creating HikariDataSource pool success.");
        return druidDataSource;
    }

    /**
     * @return One Session Jdbc DataSource
     */
    public static DruidDataSource createOneSessionJdbcDataSource(JdbcConnectionParam connectionParam) {
        logger.info("Creating OneSession DruidDataSource pool for maxActive:{}", PropertyUtils.getInt(Constants.SPRING_DATASOURCE_MAX_ACTIVE, 50));

        DruidDataSource druidDataSource = new DruidDataSource();

        druidDataSource.setDriverClassName(connectionParam.getDriverClassName());
        druidDataSource.setUrl(connectionParam.getJdbcUrl());
        druidDataSource.setUsername(connectionParam.getUser());
        druidDataSource.setPassword(PasswordUtils.decodePassword(connectionParam.getPassword()));

        druidDataSource.setMinIdle(1);
        druidDataSource.setMaxActive(1);
        druidDataSource.setTestOnBorrow(true);

        if (connectionParam.getProps() != null) {
            druidDataSource.setConnectProperties(connectionParam.getProps());
        }


        logger.info("Creating OneSession HikariDataSource pool success.");
        return druidDataSource;
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

}
