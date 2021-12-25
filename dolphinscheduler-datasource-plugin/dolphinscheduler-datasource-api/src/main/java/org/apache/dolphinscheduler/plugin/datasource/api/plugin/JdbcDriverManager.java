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

import org.apache.dolphinscheduler.spi.enums.DbType;
import org.apache.dolphinscheduler.spi.utils.StringUtils;

import org.apache.commons.collections.MapUtils;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class JdbcDriverManager {

    /**
     * datasource plugin dir
     */
    public final String pluginPath = System.getProperty("dolphinscheduler.plugin.dir");
    /**
     * datasource jdbc dir
     */
    public final String jdbcDir = System.getProperty("dolphinscheduler.jdbc.dir");

    private final Map<String, SortedMap<String, String>> jdbcDrivers = new HashMap<>();

    public JdbcDriverManager() {
        init();
    }

    private static class JdbcDriverManagerHolder {
        private static final JdbcDriverManager INSTANCE = new JdbcDriverManager();
    }

    public static JdbcDriverManager getInstance() {
        return JdbcDriverManagerHolder.INSTANCE;
    }

    /**
     * Storage of driver package
     */
    public void init() {
        if (jdbcDir != null) {
            File pluginRoot = new File(jdbcDir);
            File[] typeNames = pluginRoot.listFiles(File::isDirectory);
            if (typeNames != null) {
                for (File type : typeNames) {
                    String typeName = type.getName();
                    SortedMap<String, String> inner = jdbcDrivers.computeIfAbsent(typeName, k -> new TreeMap<>());
                    File[] jdbcFiles = type.listFiles(File::isFile);
                    if (jdbcFiles != null) {
                        Arrays.sort(jdbcFiles);
                        for (File jdbc : jdbcFiles) {
                            String jdbcName = jdbc.getName();
                            inner.put(jdbcName, jdbc.getAbsolutePath());
                        }
                    }
                }
            }
        }
    }

    /**
     * DefaultDriver Plugin Path
     */
    public String getDefaultDriverPluginPath(String typeName) {
        SortedMap<String, String> drivers = jdbcDrivers.get(typeName);
        String envDriverPath = String.format("%s/jdbc", getHiveEnv());
        if (MapUtils.isEmpty(drivers) && StringUtils.equalsIgnoreCase(typeName, DbType.HIVE.getDescp())) {
            return envDriverPath;
        }
        return drivers.get(drivers.firstKey());
    }

    public String getPluginPath(DbType type) {
        return String.format("%s/%s", this.pluginPath, type.getDescp());
    }

    public String getHiveEnv() {
        return System.getenv("HIVE_HOME") == null ? System.getenv("HIVE_CLIENT") : System.getenv("HIVE_HOME");
    }
}
