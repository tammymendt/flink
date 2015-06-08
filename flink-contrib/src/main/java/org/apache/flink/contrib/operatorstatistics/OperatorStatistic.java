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

package org.apache.flink.contrib.operatorstatistics;

import java.io.Serializable;

/**
 * Data structure that encapsulates statistical information of data that has only been processed by one pass
 * This statistical information is meant to help determine the distribution of the data that has been processed
 * in an operator so that we can determine if it is necessary to repartition the data
 *
 * The statistics to be gathered are configurable.
 *
 * The information encapsulated in this class is min, max, a structure enabling estimation of count distinct and a
 * structure holding the heavy hitters along with their frequency.
 *
 */
public abstract class OperatorStatistic implements Serializable {

	public abstract OperatorStatisticType getType();

	public abstract void add(Object newTuple);

	public abstract void merge(OperatorStatistic other);

	public abstract String toString();

	public enum OperatorStatisticType {
		CARDINALITY,
		MAX,
		MIN,
		COUNT_DISTINCT,
		HEAVY_HITTER;
	}

}