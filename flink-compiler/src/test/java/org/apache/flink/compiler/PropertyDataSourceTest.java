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

package org.apache.flink.compiler;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.translation.JavaPlan;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.compiler.plan.OptimizedPlan;
import org.apache.flink.compiler.plan.SingleInputPlanNode;
import org.apache.flink.compiler.plan.SinkPlanNode;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.util.LocalStrategy;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Test;

@SuppressWarnings({"serial"})
public class PropertyDataSourceTest extends CompilerTestBase {

	/**
	 * Tests reduce on partitioned data source
	 */
	@Test
	public void checkReduceOnPartitionedSource() {

		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
		env.setDegreeOfParallelism(DEFAULT_PARALLELISM);

		DataSet<Tuple2<Long, String>> data =
				env.readCsvFile("/some/path").types(Long.class, String.class).partitionedBy(new int[]{0});

		data.groupBy(0).reduce(new LastReduce<Tuple2<Long, String>>())
				.print();

		JavaPlan plan = env.createProgramPlan();

		// submit the plan to the compiler
		OptimizedPlan oPlan = compileNoStats(plan);

		// check the optimized Plan
		// when join should have forward strategy on both sides
		SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
		SingleInputPlanNode reduceNode = (SingleInputPlanNode) sinkNode.getPredecessor();

		ShipStrategyType reduceSS = reduceNode.getInput().getShipStrategy();
		LocalStrategy reduceLS = reduceNode.getInput().getLocalStrategy();
		DriverStrategy reducerDS = reduceNode.getDriverStrategy();

		Assert.assertEquals("Invalid ship strategy for an operator.", ShipStrategyType.FORWARD, reduceSS);
		Assert.assertEquals("Invalid local strategy for an operator.", LocalStrategy.SORT, reduceLS);
		Assert.assertEquals("Invalid driver strategy for an operator", DriverStrategy.SORTED_REDUCE, reducerDS);

	}

	/**
	 * Tests reduce on partitioned and grouped data source
	*/
	@Test
	public void checkReduceOnPartitionedGroupedSource() {

		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
		env.setDegreeOfParallelism(DEFAULT_PARALLELISM);

		DataSet<Tuple2<Long, String>> data =
				env.readCsvFile("/some/path").types(Long.class, String.class)
						.partitionedBy(new int[]{0})
						.splitsGroupedBy(new int[]{0});

		data.groupBy(0).reduce(new LastReduce<Tuple2<Long, String>>())
				.print();

		JavaPlan plan = env.createProgramPlan();

		// submit the plan to the compiler
		OptimizedPlan oPlan = compileNoStats(plan);

		// check the optimized Plan
		// when join should have forward strategy on both sides
		SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
		SingleInputPlanNode reduceNode = (SingleInputPlanNode) sinkNode.getPredecessor();

		ShipStrategyType reduceSS = reduceNode.getInput().getShipStrategy();
		LocalStrategy reduceLS = reduceNode.getInput().getLocalStrategy();
		DriverStrategy reducerDS = reduceNode.getDriverStrategy();

		Assert.assertEquals("Invalid ship strategy for an operator.", ShipStrategyType.FORWARD, reduceSS);
		Assert.assertEquals("Invalid local strategy for an operator.", LocalStrategy.NONE, reduceLS);
		Assert.assertEquals("Invalid driver strategy for an operator", DriverStrategy.SORTED_REDUCE, reducerDS);

	}

	/**
	 * Tests reduce on partitioned and sorted data source
	 */
	@Test
	public void checkReduceOnPartitionedSortedSource() {

		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
		env.setDegreeOfParallelism(DEFAULT_PARALLELISM);

		DataSet<Tuple2<Long, String>> data =
				env.readCsvFile("/some/path").types(Long.class, String.class)
						.partitionedBy(new int[]{0})
						.splitsOrderedBy(new int[]{0}, new Order[]{Order.DESCENDING});

		data.groupBy(0).reduce(new LastReduce<Tuple2<Long, String>>())
				.print();

		JavaPlan plan = env.createProgramPlan();

		// submit the plan to the compiler
		OptimizedPlan oPlan = compileNoStats(plan);

		// check the optimized Plan
		// when join should have forward strategy on both sides
		SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
		SingleInputPlanNode reduceNode = (SingleInputPlanNode) sinkNode.getPredecessor();

		ShipStrategyType reduceSS = reduceNode.getInput().getShipStrategy();
		LocalStrategy reduceLS = reduceNode.getInput().getLocalStrategy();
		DriverStrategy reducerDS = reduceNode.getDriverStrategy();

		Assert.assertEquals("Invalid ship strategy for an operator.", ShipStrategyType.FORWARD, reduceSS);
		Assert.assertEquals("Invalid local strategy for an operator.", LocalStrategy.NONE, reduceLS);
		Assert.assertEquals("Invalid driver strategy for an operator", DriverStrategy.SORTED_REDUCE, reducerDS);

	}


	/**
	 * Tests reduce on partitioned and sorted data source
	 */
	@Test
	public void checkReduceOnPartitionedSource2() {

		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
		env.setDegreeOfParallelism(DEFAULT_PARALLELISM);

		DataSet<Tuple3<Long, String, Integer>> data =
				env.readCsvFile("/some/path").types(Long.class, String.class, Integer.class)
						.partitionedBy(new int[]{1});

		data.groupBy(2, 1).reduce(new LastReduce<Tuple3<Long, String, Integer>>())
				.print();

		JavaPlan plan = env.createProgramPlan();

		// submit the plan to the compiler
		OptimizedPlan oPlan = compileNoStats(plan);

		// check the optimized Plan
		// when join should have forward strategy on both sides
		SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
		SingleInputPlanNode reduceNode = (SingleInputPlanNode) sinkNode.getPredecessor();

		ShipStrategyType reduceSS = reduceNode.getInput().getShipStrategy();
		LocalStrategy reduceLS = reduceNode.getInput().getLocalStrategy();
		DriverStrategy reducerDS = reduceNode.getDriverStrategy();

		Assert.assertEquals("Invalid ship strategy for an operator.", ShipStrategyType.FORWARD, reduceSS);
		Assert.assertEquals("Invalid local strategy for an operator.", LocalStrategy.SORT, reduceLS);
		Assert.assertEquals("Invalid driver strategy for an operator", DriverStrategy.SORTED_REDUCE, reducerDS);

	}

	/**
	 * Tests reduce on partitioned and sorted data source
	 */
	@Test
	public void checkReduceOnPartitionedSortedSource2() {

		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
		env.setDegreeOfParallelism(DEFAULT_PARALLELISM);

		DataSet<Tuple3<Long, String, Integer>> data =
				env.readCsvFile("/some/path").types(Long.class, String.class, Integer.class)
						.partitionedBy(new int[]{1})
						.splitsOrderedBy(new int[]{2,1,0}, new Order[]{Order.ASCENDING, Order.DESCENDING, Order.DESCENDING});

		data.groupBy(2, 1).reduce(new LastReduce<Tuple3<Long, String, Integer>>())
				.print();

		JavaPlan plan = env.createProgramPlan();

		// submit the plan to the compiler
		OptimizedPlan oPlan = compileNoStats(plan);

		// check the optimized Plan
		// when join should have forward strategy on both sides
		SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
		SingleInputPlanNode reduceNode = (SingleInputPlanNode) sinkNode.getPredecessor();

		ShipStrategyType reduceSS = reduceNode.getInput().getShipStrategy();
		LocalStrategy reduceLS = reduceNode.getInput().getLocalStrategy();
		DriverStrategy reducerDS = reduceNode.getDriverStrategy();

		Assert.assertEquals("Invalid ship strategy for an operator.", ShipStrategyType.FORWARD, reduceSS);
		Assert.assertEquals("Invalid local strategy for an operator.", LocalStrategy.NONE, reduceLS);
		Assert.assertEquals("Invalid driver strategy for an operator", DriverStrategy.SORTED_REDUCE, reducerDS);

	}



	public static class IdMap<T> implements MapFunction<T,T> {

		@Override
		public T map(T value) throws Exception {
			return value;
		}
	}

	public static class NoFilter<T> implements FilterFunction<T> {

		@Override
		public boolean filter(T value) throws Exception {
			return false;
		}
	}

	public static class IdFlatMap<T> implements FlatMapFunction<T,T> {

		@Override
		public void flatMap(T value, Collector<T> out) throws Exception {
			out.collect(value);
		}
	}

	public static class IdPMap<T> implements MapPartitionFunction<T,T> {

		@Override
		public void mapPartition(Iterable<T> values, Collector<T> out) throws Exception {
			for(T v : values) {
				out.collect(v);
			}
		}
	}

	public static class LastReduce<T> implements ReduceFunction<T> {

		@Override
		public T reduce(T value1, T value2) throws Exception {
			return value2;
		}
	}


}


