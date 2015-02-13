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

import com.clearspring.analytics.stream.cardinality.LinearCounting;

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
	private long tableSize;
	private long cardinality;
	private final LinearCounting[] countDistinctLC;
	//private final HyperLogLog[] countDistinctHLL;
	//private final HyperLogLogPlus[] countDistinctHLLP;


	public OperatorStatistics(OperatorStatisticsConfig config) {
		this.config = config;
		int fieldCount = config.getFieldNames().length;
		min = new Object[fieldCount];
		max = new Object[fieldCount];
		cardinality = 0L;
		tableSize = 0L;
//        countDistinctHLL = new HyperLogLog[fieldCount];
//        countDistinctHLLP = new HyperLogLogPlus[fieldCount];
		countDistinctLC = new LinearCounting[fieldCount];
		for (int i = 0; i < fieldCount; i++) {
			countDistinctLC[i] = new LinearCounting(config.getLCBitMapSize());
			//countDistinctHLL[i] = new HyperLogLog();
			//countDistinctHLLP[i] = new HyperLogLogPlus();
		}
	}

	public Object[] getMin() {
		return min;
	}

	public Object[] getMax() {
		return max;
	}

	public long getCardinality() {
		return cardinality;
	}

	public long getTableSize() {
		return tableSize;
	}

	public LinearCounting[] getCountDistinct() {
		return countDistinctLC;
	}

	/**
	 * Gathers statistical information about a record, that was i.e. emitted via a {@link org.apache.flink.util.Collector}.
	 *
	 * @param record The record to be processed
	 */
	public void process(Object record) {
		String[] fieldNames = config.getFieldNames();
		cardinality++;
//        tableSize+=(Record)record.getBinaryLength();
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
			countDistinctLC[i].offer(field);
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
