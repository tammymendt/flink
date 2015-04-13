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

import java.io.Serializable;

/**
 * Gathers statistical information about records that were i.e. emitted via a {@link org.apache.flink.util.Collector}.
 * The following information are gathered: MIN, MAX, COUNT, COUNT DISTINCT
 * For COUNT DISTINCT the objects hashcode is used.
 * Which fields of a record are to be statistically processed is determined by
 * the {@link org.apache.flink.statistics.FieldStatisticsConfig} array
 */
public class OperatorStatistics implements Serializable{

    public static final int COUNTD_BITMAP_SIZE = 1000000;
    public static final int COUNTD_LOG2M = 1000000;
    public static final int SEED = 7364181;
    public static final double HEAVY_HITTER_ERROR = 0.0001;
    public static final double HEAVY_HITTER_CONFIDENCE = 0.99;
    public static final double HEAVY_HITTER_FRACTION = 0.005;

    String taskName;
	FieldStatisticsConfig[] statsConfig;
	FieldStatistics[] stats;
	int numberOfStats;
	long totalCardinality;

	public OperatorStatistics(String taskName, FieldStatisticsConfig[] statsConfig) {
		this.taskName = taskName;
		this.numberOfStats = statsConfig.length;
		this.statsConfig = statsConfig.clone();
		stats = new FieldStatistics[numberOfStats];
		for (int i=0;i<stats.length;i++){
			stats[i] = new FieldStatistics(statsConfig[i]);
		}
		totalCardinality = 0L;
	}

	public FieldStatistics getStats(int index){
		return stats[index];
	}

	/**
	 * Gathers statistical information about a record, that was i.e. emitted via a {@link org.apache.flink.util.Collector}.
	 * Iterates over the field of the record, updating the statistics holder for that field
	 *
	 * @param record The record to be processed
	 */
	public void process(Object record) {
		for (int i = 0; i < numberOfStats; i++) {
			if (statsConfig[i].getFieldName()!=null){
				stats[i].process(getFieldValue(record,record.getClass(),statsConfig[i].getFieldName()));
			}
			//TODO - How to call a key selector function here.
			//else{ How to cast a key selector function here?
			//    stats[i].process(statsConfig[i].getKeySelector().getKey(record));
			//}
		}
		totalCardinality+=1;
	}

	private Object getFieldValue(Object record, Class<?> recordType, String fieldName) {
		try {
			return recordType.getDeclaredField(fieldName).get(record);
		} catch (NoSuchFieldException e) {
			throw new RuntimeException(String.format("Could not process record for statistics because record has no field '%s'", fieldName), e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(String.format("Could not process record for statistics, because field '%s' is not accessible", fieldName), e);
		}

	}

}
