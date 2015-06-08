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

import org.apache.flink.api.common.accumulators.Accumulator;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * This accumulator tracks an array of {@link OperatorStatistic}
 * Operator Statistics can be cardinality, min, max, and estimated values for count distinct and heavy hitters.
 * The array of {@link OperatorStatistic} is contained in a class {@link OperatorStatisticArray}
 *
 */
public class OperatorStatisticsAccumulator implements Accumulator, Serializable {

	private OperatorStatisticArray local;

	public OperatorStatisticsAccumulator(){
		local = new OperatorStatisticArray();
	}

	public OperatorStatisticsAccumulator collectMin(){
		OperatorStatisticsAccumulator newAccumulator = new OperatorStatisticsAccumulator();
		newAccumulator.local.addAll(this.local.getArray());
		newAccumulator.local.add(new MinStatistic());
		return newAccumulator;
	}

	public OperatorStatisticsAccumulator collectMax(){
		OperatorStatisticsAccumulator newAccumulator = new OperatorStatisticsAccumulator();
		newAccumulator.local.addAll(this.local.getArray());
		newAccumulator.local.add(new MaxStatistic());
		return newAccumulator;
	}

	public OperatorStatisticsAccumulator collectCountDistinct(){
		OperatorStatisticsAccumulator newAccumulator = new OperatorStatisticsAccumulator();
		newAccumulator.local.addAll(this.local.getArray());
		newAccumulator.local.add(new CountDistinctStatistic());
		return newAccumulator;
	}

	public OperatorStatisticsAccumulator collectCountDistinct(CountDistinctStatistic.CountDistinctAlgorithm algorithm){
		OperatorStatisticsAccumulator newAccumulator = new OperatorStatisticsAccumulator();
		newAccumulator.local.addAll(this.local.getArray());
		newAccumulator.local.add(new CountDistinctStatistic(algorithm));
		return newAccumulator;
	}

	public OperatorStatisticsAccumulator collectCountDistinct(CountDistinctStatistic.CountDistinctAlgorithm algorithm,int algorithmParam){
		OperatorStatisticsAccumulator newAccumulator = new OperatorStatisticsAccumulator();
		newAccumulator.local.addAll(this.local.getArray());
		newAccumulator.local.add(new CountDistinctStatistic(algorithm, algorithmParam));
		return newAccumulator;
	}

	public OperatorStatisticsAccumulator collectHeavyHitter(){
		OperatorStatisticsAccumulator newAccumulator = new OperatorStatisticsAccumulator();
		newAccumulator.local.addAll(this.local.getArray());
		newAccumulator.local.add(new HeavyHitterStatistic());
		return newAccumulator;
	}

	public OperatorStatisticsAccumulator collectHeavyHitter(HeavyHitterStatistic.HeavyHitterAlgorithm algorithm){
		OperatorStatisticsAccumulator newAccumulator = new OperatorStatisticsAccumulator();
		newAccumulator.local.addAll(this.local.getArray());
		newAccumulator.local.add(new HeavyHitterStatistic(algorithm));
		return newAccumulator;
	}

	public OperatorStatisticsAccumulator collectHeavyHitter(HeavyHitterStatistic.HeavyHitterAlgorithm algorithm,double fraction, double error){
		OperatorStatisticsAccumulator newAccumulator = new OperatorStatisticsAccumulator();
		newAccumulator.local.addAll(this.local.getArray());
		newAccumulator.local.add(new HeavyHitterStatistic(algorithm, fraction, error));
		return newAccumulator;
	}

	public OperatorStatisticsAccumulator collectHeavyHitter(HeavyHitterStatistic.HeavyHitterAlgorithm algorithm,double fraction, double error, double confidence, int seed){
		OperatorStatisticsAccumulator newAccumulator = new OperatorStatisticsAccumulator();
		newAccumulator.local.addAll(this.local.getArray());
		newAccumulator.local.add(new HeavyHitterStatistic(algorithm, fraction, error, confidence, seed));
		return newAccumulator;
	}

	public void addStatistic(OperatorStatistic operatorStatistic){
		local.add(operatorStatistic);
	}

	@Override
	public void add(Object newTuple) {
		local.addTuple(newTuple);
	}

	@Override
	public void merge(Accumulator other) {
		local.merge(((OperatorStatisticsAccumulator) other).getLocalValue());
	}


	@Override
	public OperatorStatisticsAccumulator clone() {
		OperatorStatisticsAccumulator clone = new OperatorStatisticsAccumulator();
		clone.local = new OperatorStatisticArray(new ArrayList<OperatorStatistic>(local.getArray()));
		return clone;
	}

	@Override
	public OperatorStatisticArray getLocalValue() {
		return local;
	}

	@Override
	public void resetLocal() {
		local = new OperatorStatisticArray();
	}

	public String toString(){
		return local.toString();
	}
}