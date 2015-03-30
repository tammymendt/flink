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

import org.apache.flink.api.common.functions.Function;

/**
 * Configures the behavior of an {@link org.apache.flink.statistics.FieldStatistics} instance.
 * Defines the statistics to be collected on a specific field of a Tuple
 */

public class FieldStatisticsConfig {

	//TODO how can this keySelector not be a Function but be directly a KeySelector object
	private final Function keySelector;
	private final String fieldName;

	public boolean collectMin;
	public boolean collectMax;
	public boolean collectCountDistinct;
	public CountDistinctAlgorithm countDistinctAlgorithm;

	public FieldStatisticsConfig(Function keySelector, boolean collectMin, boolean collectMax, boolean collectCountDistinct){
		this.keySelector = keySelector;
		this.fieldName = null;
		this.collectMin = collectMin;
		this.collectMax = collectMax;
		this.collectCountDistinct = collectCountDistinct;
		this.countDistinctAlgorithm = CountDistinctAlgorithm.LINEAR_COUNTING;
	}

	public FieldStatisticsConfig(String fieldName, boolean collectMin, boolean collectMax, boolean collectCountDistinct){
		this.keySelector = null;
		this.fieldName = fieldName;
		this.collectMin = collectMin;
		this.collectMax = collectMax;
		this.collectCountDistinct = collectCountDistinct;
		this.countDistinctAlgorithm = CountDistinctAlgorithm.LINEAR_COUNTING;
	}

	public FieldStatisticsConfig(Function keySelector) {
		this.keySelector = keySelector;
		this.fieldName = null;
		this.collectMin = true;
		this.collectMax = true;
		this.collectCountDistinct = true;
		this.countDistinctAlgorithm = CountDistinctAlgorithm.LINEAR_COUNTING;
	}

	public FieldStatisticsConfig(String fieldName) {
		this.fieldName = fieldName;
		this.keySelector = null;
		this.collectMin = true;
		this.collectMax = true;
		this.collectCountDistinct = true;
		this.countDistinctAlgorithm = CountDistinctAlgorithm.LINEAR_COUNTING;
	}

	public Function getKeySelector() {
		return keySelector;
	}

	public String getFieldName() {
		return fieldName;
	}

	public enum CountDistinctAlgorithm {

		LINEAR_COUNTING,
		HYPERLOGLOG;

	}

}
