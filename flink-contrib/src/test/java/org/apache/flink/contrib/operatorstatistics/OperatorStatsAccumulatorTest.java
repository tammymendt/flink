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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

public class OperatorStatsAccumulatorTest extends AbstractTestBase {

	private static final Logger LOG = LoggerFactory.getLogger(OperatorStatsAccumulatorTest.class);

	private static final String ACCUMULATOR_NAME = "op-stats";

	public OperatorStatsAccumulatorTest(){
		super(new Configuration());
	}

	@Test
	public void testAccumulator() throws Exception {

		String input = "";

		Random rand = new Random();

		for (int i = 1; i < 1000; i++) {
			if(rand.nextDouble()<0.2){
				input+=String.valueOf(rand.nextInt(4))+"\n";
			}else{
				input+=String.valueOf(rand.nextInt(100))+"\n";
			}
		}

		String inputFile = createTempFile("datapoints.txt", input);

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env.readTextFile(inputFile).
				flatMap(new StringToInt()).
				output(new DiscardingOutputFormat<Tuple1<Integer>>());

		JobExecutionResult result = env.execute();

		OperatorStatisticArray globalStats = result.getAccumulatorResult(ACCUMULATOR_NAME);
		System.out.println("Global Stats");
		System.out.println(globalStats.toString());

		OperatorStatisticArray merged = new OperatorStatisticArray();

		Map<String,Object> accResults = result.getAllAccumulatorResults();
		for (String accumulatorName:accResults.keySet()){
			if (accumulatorName.contains(ACCUMULATOR_NAME+"-")){
				OperatorStatisticArray localStats =  (OperatorStatisticArray)accResults.get(accumulatorName);
				System.out.println("Local Stats: " + accumulatorName);
				System.out.println(localStats.toString());
				if (merged.getArray().isEmpty()){
					merged.addAll(localStats.getArray());
				}else{
					merged.merge(localStats);
				}
			}
		}

		System.out.println("Local Stats Merged: \n");
		System.out.println(merged.toString());

		Assert.assertEquals("Global cardinality should be 999", 999, globalStats.getCardinality());
		Assert.assertEquals("Count distinct should be around 100 (+/- 5)", 100.0, (double)globalStats.getCountDistinctEstimate(),5.0);
		Assert.assertTrue("The total number of heavy hitters should be between 0 and 5"
				, globalStats.getHeavyHitters().size() > 0 && globalStats.getHeavyHitters().size() <= 5);
		Assert.assertEquals("Min when merging the local accumulators should correspond with min" +
				"of the global accumulator", merged.getMin(), globalStats.getMin());
		Assert.assertEquals("Max resulting from merging the local accumulators should correspond to" +
				"max of the global accumulator",merged.getMax(),globalStats.getMax());
		Assert.assertEquals("Count distinct when merging the local accumulators should correspond to " +
				"count distinct in the global accumulator",merged.getCountDistinctEstimate(),globalStats.getCountDistinctEstimate());
		Assert.assertEquals("The number of heavy hitters when merging the local accumulators should correspond " +
				"to the number of heavy hitters in the global accumulator",merged.getHeavyHitters().size(),globalStats.getHeavyHitters().size());
	}

	public static class StringToInt extends RichFlatMapFunction<String, Tuple1<Integer>> {

		private OperatorStatisticsAccumulator globalAccumulator;
		private OperatorStatisticsAccumulator localAccumulator;

		@Override
		public void open(Configuration parameters) {

			globalAccumulator = (OperatorStatisticsAccumulator)getRuntimeContext().getAccumulator(ACCUMULATOR_NAME);
			if (globalAccumulator==null){
				globalAccumulator = new OperatorStatisticsAccumulator()
									.collectMin()
									.collectMax()
									.collectCountDistinct()
									.collectHeavyHitter();
				getRuntimeContext().addAccumulator(ACCUMULATOR_NAME, globalAccumulator);
			}

			int subTaskIndex = getRuntimeContext().getIndexOfThisSubtask();
			localAccumulator = (OperatorStatisticsAccumulator)getRuntimeContext().getAccumulator(ACCUMULATOR_NAME+"-"+subTaskIndex);
			if (localAccumulator==null){
				localAccumulator = new OperatorStatisticsAccumulator()
									.collectMin()
									.collectMax()
									.collectCountDistinct()
									.collectHeavyHitter();
				getRuntimeContext().addAccumulator(ACCUMULATOR_NAME+"-"+subTaskIndex, localAccumulator);
			}
		}

		@Override
		public void flatMap(String value, Collector<Tuple1<Integer>> out) throws Exception {
			int intValue;
			try {
				intValue = Integer.parseInt(value);
				localAccumulator.add(intValue);
				out.collect(new Tuple1(intValue));
			} catch (NumberFormatException ex) {
			}
		}

		@Override
		public void close(){
			globalAccumulator.merge(localAccumulator);
		}
	}

}