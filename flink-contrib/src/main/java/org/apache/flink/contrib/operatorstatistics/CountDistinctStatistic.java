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

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.clearspring.analytics.stream.cardinality.LinearCounting;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class CountDistinctStatistic extends OperatorStatistic {

	private static final OperatorStatisticType OP_STAT_TYPE = OperatorStatisticType.COUNT_DISTINCT ;
	CountDistinctConfig config;
	transient ICardinality countDistinct;

	public CountDistinctStatistic(){
		this.config = new CountDistinctConfig();
		initializeCountDistinct();
	}

	public CountDistinctStatistic(CountDistinctAlgorithm algorithm){
		this.config = new CountDistinctConfig(algorithm);
		initializeCountDistinct();
	}

	public CountDistinctStatistic(CountDistinctAlgorithm algorithm, int algorithmParam){
		this.config = new CountDistinctConfig(algorithm, algorithmParam);
		initializeCountDistinct();
	}

	private void initializeCountDistinct(){
		if (config.countDistinctAlgorithm.equals(CountDistinctAlgorithm.LINEAR_COUNTING)) {
			countDistinct = new LinearCounting(config.bitmapSize);
		}
		if(config.countDistinctAlgorithm.equals(CountDistinctAlgorithm.HYPERLOGLOG)){
			countDistinct = new HyperLogLog(config.log2m);
		}
	}

	public long estimateCountDistinct(){
		return countDistinct.cardinality();
	}

	@Override
	public void add(Object tupleObject){
		countDistinct.offer(tupleObject);
	}

	@Override
	public void merge(OperatorStatistic other){

		try {
			ICardinality mergedCountDistinct = countDistinct.merge(new ICardinality[]{countDistinct,((CountDistinctStatistic)other).countDistinct});
			countDistinct = mergedCountDistinct;
		} catch (CardinalityMergeException e) {
			throw new RuntimeException("Error merging count distinct structures",e);
		}
	}

	@Override
	public String toString(){
		return "\ncount distinct estimate("+config.countDistinctAlgorithm+"): " + countDistinct.cardinality();
	}

	private void writeObject(ObjectOutputStream out) throws IOException {
		out.defaultWriteObject();
		if (config.countDistinctAlgorithm.equals(CountDistinctAlgorithm.LINEAR_COUNTING)){
			out.writeObject(countDistinct.getBytes());
		}else{
			out.writeObject(countDistinct);
		}
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		if (config.countDistinctAlgorithm.equals(CountDistinctAlgorithm.LINEAR_COUNTING)){
			countDistinct = new LinearCounting((byte[])in.readObject());
		}else{
			countDistinct = (HyperLogLog)in.readObject();
		}
	}

	@Override
	public OperatorStatisticType getType(){
		return OP_STAT_TYPE;
	}

	public class CountDistinctConfig implements Serializable {

		//Default Configuration values for tracking Count Distinct
		private int bitmapSize = 1000000;
		private int log2m = 10;
		private CountDistinctAlgorithm countDistinctAlgorithm = CountDistinctAlgorithm.HYPERLOGLOG;

		public CountDistinctConfig(){}

		public CountDistinctConfig(CountDistinctAlgorithm algorithm){
			countDistinctAlgorithm = algorithm;
		}

		public CountDistinctConfig(CountDistinctAlgorithm algorithm, int algorithmParam){
			countDistinctAlgorithm = algorithm;
			if (countDistinctAlgorithm.equals(CountDistinctAlgorithm.HYPERLOGLOG)){
				log2m = algorithmParam;
			}else{
				bitmapSize = algorithmParam;
			}
		}
	}

	public enum CountDistinctAlgorithm {
		LINEAR_COUNTING,
		HYPERLOGLOG;
	}
}
