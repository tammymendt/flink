/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.statistics;

/**
 * Configures the behavior of an {@link org.apache.flink.statistics.OperatorStatistics} instance. The fields of the record that shall be used to gather statistics can be defined here.
 */
public class OperatorStatisticsConfig {

    private final Class<?> recordType;
    private final String[] fieldNames;

    public OperatorStatisticsConfig(Class<?> recordType, String[] fieldNames) {
        this.recordType = recordType;
        this.fieldNames = fieldNames;
    }

    public Class<?> getRecordType() {
        return recordType;
    }

    public String[] getFieldNames() {
        return fieldNames;
    }
}
