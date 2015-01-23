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

package org.apache.flink.runtime.operators.shipping;

import org.apache.flink.statistics.OperatorStatistics;
import org.apache.flink.statistics.OperatorStatisticsConfig;
import org.apache.flink.util.Collector;

/**
 * Wraps an implementation of the {@link org.apache.flink.util.Collector} interface
 * and gathers statistical information about the emitted records.
 */
public class StatisticsCollectorWrapper<T> implements Collector<T> {

    private Collector<T> collector;

    private String fieldName;
    private Class<T> recordType;

    private final OperatorStatistics stats;

    public StatisticsCollectorWrapper(Collector<T> collector, OperatorStatisticsConfig config) {
        this.collector = collector;
        this.stats = new OperatorStatistics(config);
    }

    @Override
    public void collect(T record) {
        collector.collect(record);
        stats.process(record);
    }

    @Override
    public void close() {
        collector.close();
    }


}
