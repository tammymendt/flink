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

import org.apache.flink.contrib.operatorstatistics.heavyhitters.CountMinHeavyHitter;
import org.apache.flink.contrib.operatorstatistics.heavyhitters.HeavyHitter;
import org.apache.flink.contrib.operatorstatistics.heavyhitters.HeavyHitterMergeException;
import org.apache.flink.contrib.operatorstatistics.heavyhitters.LossyCounting;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;

public class HeavyHitterStatistic extends OperatorStatistic {

	private static final OperatorStatisticType OP_STAT_TYPE = OperatorStatisticType.HEAVY_HITTER;
	HeavyHitterConfig config;
	transient HeavyHitter heavyHitter;

	public HeavyHitterStatistic() {
		this.config = new HeavyHitterConfig();
		initializeHeavyHitters();
	}

	public HeavyHitterStatistic(HeavyHitterAlgorithm algorithm) {
		this.config = new HeavyHitterConfig(algorithm);
		initializeHeavyHitters();
	}

	public HeavyHitterStatistic(HeavyHitterAlgorithm algorithm, double fraction, double error) {
		this.config = new HeavyHitterConfig(algorithm, fraction, error);
		initializeHeavyHitters();
	}

	public HeavyHitterStatistic(HeavyHitterAlgorithm algorithm, double fraction, double error, double confidence, int seed) {
		this.config = new HeavyHitterConfig(algorithm, fraction, error, confidence, seed);
		initializeHeavyHitters();
	}

	private void initializeHeavyHitters(){
		if (config.heavyHitterAlgorithm.equals(HeavyHitterAlgorithm.LOSSY_COUNTING)) {
			heavyHitter = new LossyCounting(config.fraction, config.error);
		}
		if (config.heavyHitterAlgorithm.equals(HeavyHitterAlgorithm.COUNT_MIN_SKETCH)) {
			heavyHitter = new CountMinHeavyHitter(config.fraction,
					config.error,
					config.confidence,
					config.seed);
		}
	}

	@Override
	public void add(Object value) {
		heavyHitter.addObject(value);
	}

	@Override
	public void merge(OperatorStatistic other) {
		try {
			this.heavyHitter.merge(((HeavyHitterStatistic) other).heavyHitter);
		} catch (HeavyHitterMergeException e) {
			throw new RuntimeException("Error merging heavy hitter structures", e);
		}
	}

	@Override
	public String toString() {
		return "\nheavy hitters (" + config.heavyHitterAlgorithm + "):\n" + heavyHitter.toString();
	}

	private void writeObject(ObjectOutputStream out) throws IOException {
		out.defaultWriteObject();
		out.writeObject(heavyHitter);
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		if (config.heavyHitterAlgorithm.equals(HeavyHitterAlgorithm.LOSSY_COUNTING)) {
			heavyHitter = (LossyCounting) in.readObject();
		} else {
			heavyHitter = (CountMinHeavyHitter) in.readObject();
		}
	}

	@Override
	public OperatorStatisticType getType() {
		return OP_STAT_TYPE;
	}

	public Map<Object, Long> getHeavyHitters() {
		return heavyHitter.getHeavyHitters();
	}

	public class HeavyHitterConfig implements Serializable {

		//Default Configuration values for tracking Heavy Hitters
		public int seed = 121311332;
		public double confidence = 0.99;
		public double fraction = 0.05;
		public double error = 0.0005;
		public HeavyHitterAlgorithm heavyHitterAlgorithm = HeavyHitterAlgorithm.COUNT_MIN_SKETCH;

		public HeavyHitterConfig() {}

		public HeavyHitterConfig(HeavyHitterAlgorithm algorithm) {
			heavyHitterAlgorithm = algorithm;
		}

		public HeavyHitterConfig(HeavyHitterAlgorithm algorithm, double fraction, double error) {
			heavyHitterAlgorithm = algorithm;
			this.fraction = fraction;
			this.error = error;
		}

		public HeavyHitterConfig(HeavyHitterAlgorithm algorithm, double fraction, double error, double confidence, int seed) {
			heavyHitterAlgorithm = algorithm;
			this.fraction = fraction;
			this.error = error;
			this.confidence = confidence;
			this.seed = seed;
		}

	}

	public enum HeavyHitterAlgorithm{
		LOSSY_COUNTING,
		COUNT_MIN_SKETCH;
	}
}