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

package org.apache.dolphinscheduler.api.service;

import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.spi.datasource.DataSourceParam;
import org.apache.dolphinscheduler.spi.datasource.JdbcConnectionParam;

import java.util.Map;

/**
 * data source service
 */
public interface DataSourceService {

    /**
     * create data source
     *
     * @param loginUser login user
     * @param datasourceParam datasource parameter
     * @return create result code
     */
    Result<Object> createDataSource(User loginUser, DataSourceParam datasourceParam);

    /**
     * updateProcessInstance datasource
     *
     * @param loginUser login user
     * @param id data source id
     * @param dataSourceParam data source params
     * @return update result code
     */
    Result<Object> updateDataSource(int id, User loginUser, DataSourceParam dataSourceParam);

    /**
     * updateProcessInstance datasource
     *
     * @param id datasource id
     * @return data source detail
     */
    Map<String, Object> queryDataSource(int id);

    /**
     * query datasource list by keyword
     *
     * @param loginUser login user
     * @param searchVal search value
     * @param pageNo    page number
     * @param pageSize  page size
     * @return data source list page
     */
    Result queryDataSourceListPaging(User loginUser, String searchVal, Integer pageNo, Integer pageSize);

    /**
     * query data resource list
     *
     * @param loginUser login user
     * @param type      data source type
     * @return data source list page
     */
    Map<String, Object> queryDataSourceList(User loginUser, Integer type);

    /**
     * verify datasource exists
     *
     * @param name      datasource name
     * @return true if data datasource not exists, otherwise return false
     */
    Result<Object> verifyDataSourceName(String name);

    /**
     * check connection
     *
     * @param jdbcConnectionParam  JDBC connection parameters
     * @return true if connect successfully, otherwise false
     */
    Result<Object> checkConnection(JdbcConnectionParam jdbcConnectionParam);

    /**
     * test connection
     *
     * @param id datasource id
     * @return connect result code
     */
    Result<Object> connectionTest(int id);

    /**
     * delete datasource
     *
     * @param loginUser    login user
     * @param datasourceId data source id
     * @return delete result code
     */
    Result<Object> delete(User loginUser, int datasourceId);

    /**
     * unauthorized datasource
     *
     * @param loginUser login user
     * @param userId    user id
     * @return unauthed data source result code
     */
    Map<String, Object> unauthDatasource(User loginUser, Integer userId);

    /**
     * authorized datasource
     *
     * @param loginUser login user
     * @param userId    user id
     * @return authorized result code
     */
    Map<String, Object> authedDatasource(User loginUser, Integer userId);
}
