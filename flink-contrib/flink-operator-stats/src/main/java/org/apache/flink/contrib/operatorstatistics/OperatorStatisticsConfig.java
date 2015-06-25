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
 * Configures the behavior of an {@link OperatorStatistics} instance.
 *
 * Sets the parameters that determine the accuracy of the count distinct and heavy hitter sketches
 *
 * Defines the statistics to be collected. A boolean field indicates whether a given statistic should be collected or not
 *
 * Encapsulates an enum indicating which sketch should be used for count distinct and another indicating which sketch
 * should be used for detecting heavy hitters.
 */

public class OperatorStatisticsConfig implements Serializable {

	public static int countDbitmapSize = 1000000;
	public static int countDlog2m = 20;

	public static int heavyHitterSeed = 121311332;
	public static double heavyHitterConfidence = 0.99;
	public static double heavyHitterFraction = 0.05;
	public static double heavyHitterError = 0.0005;

	public boolean collectMin = false;
	public boolean collectMax = false;
	public boolean collectCountDistinct = false;
	public boolean collectHeavyHitters = false;
	public CountDistinctAlgorithm countDistinctAlgorithm;
	public HeavyHitterAlgorithm heavyHitterAlgorithm;

	public OperatorStatisticsConfig(boolean trackStats){
		if (trackStats){
			this.collectMin = true;
			this.collectMax = true;
			this.collectCountDistinct = true;
			this.collectHeavyHitters = true;
			this.countDistinctAlgorithm = CountDistinctAlgorithm.HYPERLOGLOG;
			this.heavyHitterAlgorithm = HeavyHitterAlgorithm.LOSSY_COUNTING;
		} else {
			this.collectMin = false;
			this.collectMax = false;
			this.collectCountDistinct = false;
			this.collectHeavyHitters = false;
		}
	}

	public OperatorStatisticsConfig(CountDistinctAlgorithm countDistinct, HeavyHitterAlgorithm heavyHitter) {
		this.collectMin = true;
		this.collectMax = true;
		this.collectCountDistinct = true;
		this.collectHeavyHitters = true;
		this.countDistinctAlgorithm = countDistinct;
		this.heavyHitterAlgorithm = heavyHitter;
	}

	public enum CountDistinctAlgorithm {

		LINEAR_COUNTING,
		HYPERLOGLOG;

	}

	public enum HeavyHitterAlgorithm{
		LOSSY_COUNTING,
		COUNT_MIN_SKETCH;
	}

	public static void setHeavyHitterError(double heavyHitterError) {
		OperatorStatisticsConfig.heavyHitterError = heavyHitterError;
	}

	public static void setCountDbitmapSize(int countDbitmapSize) {
		OperatorStatisticsConfig.countDbitmapSize = countDbitmapSize;
	}

	public static void setCountDlog2m(int countDlog2m) {
		OperatorStatisticsConfig.countDlog2m = countDlog2m;
	}

	public static void setHeavyHitterSeed(int heavyHitterSeed) {
		OperatorStatisticsConfig.heavyHitterSeed = heavyHitterSeed;
	}

	public static void setHeavyHitterConfidence(double heavyHitterConfidence) {
		OperatorStatisticsConfig.heavyHitterConfidence = heavyHitterConfidence;
	}

	public static void setHeavyHitterFraction(double heavyHitterFraction) {
		OperatorStatisticsConfig.heavyHitterFraction = heavyHitterFraction;
	}
}
