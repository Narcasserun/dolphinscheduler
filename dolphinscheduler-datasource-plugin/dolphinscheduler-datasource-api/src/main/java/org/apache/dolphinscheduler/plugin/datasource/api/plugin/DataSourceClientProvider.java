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

package org.apache.dolphinscheduler.plugin.datasource.api.plugin;

import org.apache.dolphinscheduler.plugin.datasource.api.exception.DataSourceException;
import org.apache.dolphinscheduler.plugin.datasource.api.utils.ClassLoaderUtils;
import org.apache.dolphinscheduler.plugin.datasource.api.utils.ReflectionUtils;
import org.apache.dolphinscheduler.spi.datasource.DataSourceClient;
import org.apache.dolphinscheduler.spi.datasource.JdbcConnectionParam;
import org.apache.dolphinscheduler.spi.enums.DbType;
import org.apache.dolphinscheduler.spi.utils.StringUtils;

import java.io.FilenameFilter;
import java.net.MalformedURLException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.Driver;
import java.util.HashSet;
import java.util.Iterator;
import java.util.ServiceLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class DataSourceClientProvider {
    private static final Logger logger = LoggerFactory.getLogger(DataSourceClientProvider.class);

    private static final String BASE_PACKAGE = "org.apache.dolphinscheduler.plugin.datasource."+"hive";
    private static final JdbcDriverManager jdbcDriverManagerInstance = JdbcDriverManager.getInstance();

    public static DataSourceClient createDataSourceClient(JdbcConnectionParam connectionParam) {
        logger.info("Creating the createDataSourceClient. JdbcUrl: {} ", connectionParam.getJdbcUrl());

        //Check jdbc driver location
        checkDriverLocation(connectionParam);

        logger.info("Creating the ClassLoader for the jdbc driver and plugin.");
        ClassLoader driverClassLoader = getDriverClassLoader(connectionParam);

        ClassLoader threadClassLoader = Thread.currentThread().getContextClassLoader();

        try {
            Thread.currentThread().setContextClassLoader(driverClassLoader);
            return createDataSourceClientWithClassLoader(connectionParam, driverClassLoader);
        } finally {
            Thread.currentThread().setContextClassLoader(threadClassLoader);
        }
//        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader()) {
//            return createDataSourceClientWithClassLoader(connectionParam);
//        }

    }

    protected static void checkDriverLocation(JdbcConnectionParam connectionParam) {
        final String driverLocation = connectionParam.getDriverLocation();
        if (StringUtils.isBlank(driverLocation)) {
            logger.warn("No jdbc driver provide,will use randomly driver jar for {}.", connectionParam.getDbType().getDescp());
            connectionParam.setDriverLocation(jdbcDriverManagerInstance.getDefaultDriverPluginPath(connectionParam.getDbType().getDescp()));
        }
    }

    protected static ClassLoader getDriverClassLoader(JdbcConnectionParam connectionParam) {
        FilenameFilter filenameFilter = (dir, name) -> name != null && name.endsWith(".jar");
        ClassLoader threadClassLoader = Thread.currentThread().getContextClassLoader();
        ClassLoader classLoader;

        String locationString = connectionParam.getDriverLocation();
        logger.info("Driver location: {}", locationString);
        HashSet<String> paths = Sets.newHashSet(locationString);
        try {
            classLoader = ClassLoaderUtils.getCustomClassLoader(paths, threadClassLoader, filenameFilter);
        } catch (final MalformedURLException e) {
            throw DataSourceException.getInstance("Invalid jdbc driver location.", e);
        }

        //try loading jdbc driver
        loadJdbcDriver(classLoader, connectionParam);

        DbType dbType = connectionParam.getDbType();
        String pluginPath = JdbcDriverManager.getInstance().getPluginPath(dbType);
        logger.info("Plugin location: {}", pluginPath);
        paths.add(pluginPath);

        if (dbType == DbType.HIVE || dbType == DbType.SPARK) {
            try {
                Class.forName("org.apache.hadoop.conf.Configuration", true, classLoader);
                Class.forName("org.apache.hadoop.security.UserGroupInformation", true, classLoader);
                Class.forName("org.apache.hadoop.fs.FileSystem", true, classLoader);
            } catch (ClassNotFoundException cnf) {
                cnf.printStackTrace();
            }
        }

        try {
            classLoader = ClassLoaderUtils.getCustomClassLoader(paths, threadClassLoader, filenameFilter);
        } catch (final MalformedURLException e) {
            throw DataSourceException.getInstance("Plugin classpath init error.");
        }
        logger.info("Create PluginClassLoader Success {}", classLoader.toString());
        return classLoader;
    }

    /**
     * Actively load driver and register, If it cannot be loaded, the driver is loaded through SPI
     */
    protected static void loadJdbcDriver(ClassLoader classLoader, JdbcConnectionParam connectionParam) {
        Boolean loaded;
        String drv = connectionParam.getDriverClassName();
        try {
            final Class<?> clazz = Class.forName(drv, true, classLoader);
            final Driver driver = (Driver) clazz.newInstance();
            if (!driver.acceptsURL(connectionParam.getJdbcUrl())) {
                logger.warn("{} : Driver {} cannot accept url.", "Jdbc driver loading error", drv);
                throw DataSourceException.getInstance("Jdbc driver loading error");
            }
            logger.info("Loader jdbc driver {} success.", drv);
            loaded = Boolean.TRUE;
        } catch (final Exception e) {
            logger.warn("The specified driver not suitable.Try SPI loading driver.");
            loaded = AccessController.doPrivileged((PrivilegedAction<Boolean>) () -> {
                ServiceLoader<Driver> loadedDrivers = ServiceLoader.load(Driver.class, classLoader);
                Iterator<Driver> driversIterator = loadedDrivers.iterator();
                try {
                    while (driversIterator.hasNext()) {
                        Driver driver = driversIterator.next();
                        if (driver.acceptsURL(connectionParam.getJdbcUrl())) {
                            connectionParam.setVersion(driver.getMajorVersion());
                            connectionParam.setDriverClassName(driver.getClass().getName());
                            logger.info("Loader jdbc driver {} success.Major version number: {}", drv, driver.getMajorVersion());
                            return Boolean.TRUE;
                        }
                    }
                } catch (Throwable t) {
                    logger.warn("Ignoring java.sql.Driver classes listed in resources but not"
                            + " present in class loader's classpath: ", t);
                }
                return Boolean.FALSE;
            });
        }
        if (!loaded) {
            throw DataSourceException.getInstance("Jdbc driver loading error");
        }
    }

//    private static DataSourceClient createDataSourceClientWithClassLoader(JdbcConnectionParam connectionParam) {
//        ServiceLoader<DataSourceChannelFactory> serviceLoader = ServiceLoader.load(DataSourceChannelFactory.class);
//        List<DataSourceChannelFactory> plugins = ImmutableList.copyOf(serviceLoader);
//        Preconditions.checkState(!plugins.isEmpty(), "No service providers the plugin %s", DataSourceClient.class.getName());
//        DataSourceClient dataSourceClient = null;
//        for (DataSourceChannelFactory dataSourceChannelFactory : plugins) {
//            logger.info("Installing {}", dataSourceChannelFactory.getClass().getName());
//            dataSourceClient = dataSourceChannelFactory.create().createDataSourceClient(connectionParam);
//        }
//        return dataSourceClient;
//    }

    protected static DataSourceClient createDataSourceClientWithClassLoader(JdbcConnectionParam connectionParam, ClassLoader classLoader) {

        Class<?> dataSourceClientClass;
        DataSourceClient dataSourceClient;
        try {
            switch (connectionParam.getDbType()) {
                case MYSQL:
                    dataSourceClientClass = Class.forName("org.apache.dolphinscheduler.plugin.datasource.mysql.MySQLDataSourceClient", true, classLoader);
                    break;
                case POSTGRESQL:
                    dataSourceClientClass = Class.forName(String.format("%sPostgreSQLDataSourceClient", BASE_PACKAGE), true, classLoader);
                    break;
                case HIVE:
                case SPARK:
                    dataSourceClientClass = Class.forName("org.apache.dolphinscheduler.plugin.datasource.hive.HiveDataSourceClient", true, classLoader);
                    break;
                case CLICKHOUSE:
                    dataSourceClientClass = Class.forName(String.format("%sClickhouseDataSourceClient", BASE_PACKAGE), true, classLoader);
                    break;
                case ORACLE:
                    dataSourceClientClass = Class.forName(String.format("%sOracleDataSourceClient", BASE_PACKAGE), true, classLoader);
                    break;
                case SQLSERVER:
                    dataSourceClientClass = Class.forName(String.format("%sSqlserverDataSourceClient", BASE_PACKAGE), true, classLoader);
                    break;
                case DB2:
                    dataSourceClientClass = Class.forName(String.format("%sDB2DataSourceClient", BASE_PACKAGE), true, classLoader);
                    break;
                default:
                    throw DataSourceException.getInstance(String.format("datasource plugin '%s' is not found", connectionParam.getDbType().getDescp()));
            }
            logger.info("Reflection: {}", dataSourceClientClass);
            dataSourceClient = (DataSourceClient) ReflectionUtils.newInstance(dataSourceClientClass, connectionParam);
        } catch (Exception e) {
            throw DataSourceException.getInstance("Datasource plugin initialize fail", e);
        }
        logger.info("Create DataSourceClient {} for {} success.", connectionParam.getJdbcUrl(), connectionParam.getDbType().getDescp());
        return dataSourceClient;
    }
}
