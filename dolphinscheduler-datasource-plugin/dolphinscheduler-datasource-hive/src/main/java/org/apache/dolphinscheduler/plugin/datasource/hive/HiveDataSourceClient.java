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

package org.apache.dolphinscheduler.plugin.datasource.hive;

import static org.apache.dolphinscheduler.spi.task.TaskConstants.JAVA_SECURITY_KRB5_CONF;

import org.apache.dolphinscheduler.plugin.datasource.api.client.CommonDataSourceClient;
import org.apache.dolphinscheduler.plugin.datasource.api.exception.DataSourceException;
import org.apache.dolphinscheduler.plugin.datasource.api.provider.JdbcDataSourceProvider;
import org.apache.dolphinscheduler.plugin.datasource.utils.CommonUtil;
import org.apache.dolphinscheduler.spi.datasource.JdbcConnectionParam;
import org.apache.dolphinscheduler.spi.utils.StringUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.security.krb5.Config;

public class HiveDataSourceClient extends CommonDataSourceClient {

    private static final Logger logger = LoggerFactory.getLogger(HiveDataSourceClient.class);

    private ScheduledExecutorService kerberosRenewalService;

    private Configuration hadoopConf;
    protected DataSource oneSessionDataSource;
    private UserGroupInformation ugi;

    public HiveDataSourceClient(JdbcConnectionParam connectionParam) {
        super(connectionParam);
    }

    @Override
    protected void preInit() {
        logger.info("PreInit in {}", getClass().getName());
        this.kerberosRenewalService = Executors.newSingleThreadScheduledExecutor();
    }

    @Override
    protected void initClient(JdbcConnectionParam connectionParam) {
        logger.info("Create Configuration for hive configuration.");
        this.hadoopConf = createHadoopConf();
        logger.info("Create Configuration success.");

        logger.info("Create UserGroupInformation.");
        this.ugi = createUserGroupInformation(connectionParam);
        logger.info("Create ugi success.");

        super.initClient(connectionParam);
        this.oneSessionDataSource = JdbcDataSourceProvider.getDataSourceFactory().createDataSource(connectionParam);
        logger.info("Init {} success.", getClass().getName());
    }

    @Override
    protected void checkEnv(JdbcConnectionParam connectionParam) {
        super.checkEnv(connectionParam);
        checkKerberosEnv(connectionParam);
    }

    private void checkKerberosEnv(JdbcConnectionParam connectionParam) {
        if (StringUtils.isNotBlank(connectionParam.getKerberosKrb5Conf())) {
            System.setProperty(JAVA_SECURITY_KRB5_CONF, connectionParam.getKerberosKrb5Conf());
            try {
                Config.refresh();
                Class<?> kerberosName = Class.forName("org.apache.hadoop.security.authentication.util.KerberosName");
                Field field = kerberosName.getDeclaredField("defaultRealm");
                field.setAccessible(true);
                field.set(null, Config.getInstance().getDefaultRealm());
            } catch (Exception e) {
                throw DataSourceException.getInstance("Update Kerberos environment failed.", e);
            }
        }
    }

    private UserGroupInformation createUserGroupInformation(JdbcConnectionParam connectionParam) {
        try {
            UserGroupInformation ugi = CommonUtil.createUGI(getHadoopConf(), connectionParam);
            try {
                Field isKeytabField = ugi.getClass().getDeclaredField("isKeytab");
                isKeytabField.setAccessible(true);
                isKeytabField.set(ugi, true);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                logger.warn(e.getMessage());
            }

            kerberosRenewalService.scheduleWithFixedDelay(() -> {
                try {
                    ugi.checkTGTAndReloginFromKeytab();
                } catch (IOException e) {
                    logger.error("Check TGT and Renewal from Keytab error", e);
                }
            }, 5, 5, TimeUnit.MINUTES);
            return ugi;
        } catch (IOException e) {
            throw DataSourceException.getInstance("createUserGroupInformation fail. ", e);
        }
    }

    protected Configuration createHadoopConf() {
        Configuration hadoopConf = new Configuration();
        hadoopConf.setBoolean("ipc.client.fallback-to-simple-auth-allowed", true);
        return hadoopConf;
    }

    protected Configuration getHadoopConf() {
        return this.hadoopConf;
    }

    @Override
    public Connection getConnection() {
        try {
            return oneSessionDataSource.getConnection();
        } catch (SQLException e) {
            logger.error("get oneSessionDataSource Connection fail SQLException: {}", e.getMessage(), e);
            return null;
        }
    }

    @Override
    public void close() {
        super.close();

        logger.info("close HiveDataSourceClient.");
        kerberosRenewalService.shutdown();
        this.ugi = null;
        JdbcDataSourceProvider.getDataSourceFactory().destroy(this.dataSource);
        this.oneSessionDataSource = null;
    }
}
