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

import java.util.HashSet;
import java.util.Set;

/**
 * Gathers statistical information about records that were i.e. emitted via a {@link org.apache.flink.util.Collector}.
 * The following information are gathered: MIN, MAX, COUNT, COUNT DISTINCT
 * For COUNT DISTINCT the objects hashcode is used.
 * Which fields of a record are to be statistically processed is determined by the {@link org.apache.flink.statistics.OperatorStatisticsConfig}
 */
public class OperatorStatistics {

    /**
     * The config that defines which from which fields statistics are to be gathered
     */
    private final OperatorStatisticsConfig config;

    /*
        actual statistics
     */
    private final Object[] min;
    private final Object[] max;
    private final long[] count;
    private final long[] countDistinct;

    /**
     * used to determine countDistinct: each records hashcode will be added
     */
    private Set<Integer>[] recordHashes;

    public OperatorStatistics(OperatorStatisticsConfig config) {
        this.config = config;
        int fieldCount = config.getFieldNames().length;
        min = new Object[fieldCount];
        max = new Object[fieldCount];
        count = new long[fieldCount];
        countDistinct = new long[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            count[i] = 0L;
            countDistinct[i] = 0L;
            recordHashes[i] = new HashSet<Integer>();
        }
    }

    public Object[] getMin() {
        return min;
    }

    public Object[] getMax() {
        return max;
    }

    public long[] getCount() {
        return count;
    }

    public long[] getCountDistinct() {
        return countDistinct;
    }

    /**
     * Gathers statistical information about a record, that was i.e. emitted via a {@link org.apache.flink.util.Collector}.
     *
     * @param record The record to be processed
     */
    public void process(Object record) {
        String[] fieldNames = config.getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            Object field = getFieldValue(record, config.getRecordType(), fieldNames[i]);
            if (field instanceof Comparable) {
                Comparable compField = (Comparable) field;
                if (min[i] == null || compField.compareTo(min[i]) < 0) {
                    min[i] = field;
                }
                if (max[i] == null || compField.compareTo(max[i]) > 0) {
                    max[i] = field;
                }
            }
            count[i]++;
            if (!recordHashes[i].contains(field.hashCode())) {
                countDistinct[i]++;
                recordHashes[i].add(field.hashCode());
            }
        }

    }

    private Object getFieldValue(Object record, Class<?> recordType, String fieldName) {
        try {
            return recordType.getField(fieldName).get(record);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(String.format("Could not process record for statistics because record has no field '%s'", fieldName), e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(String.format("Could not process record for statistics, because field '%s' is not accessible", fieldName), e);
        }

    }

}
