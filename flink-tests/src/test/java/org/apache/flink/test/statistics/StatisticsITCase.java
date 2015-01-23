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

package org.apache.flink.test.statistics;

import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.api.common.typeutils.record.RecordSerializerFactory;
import org.apache.flink.api.java.record.functions.MapFunction;
import org.apache.flink.api.java.record.io.CsvInputFormat;
import org.apache.flink.api.java.record.io.CsvOutputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.*;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.operators.CollectorMapDriver;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.RegularPactTask;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.util.LocalStrategy;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.test.iterative.nephele.JobGraphUtils;
import org.apache.flink.test.util.RecordAPITestBase;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;

@SuppressWarnings("deprecation")
public class StatisticsITCase extends RecordAPITestBase {

	protected String inputPath;

	protected String resultPath;
	

	public static final String getInputString() {
		
		/*
		 *  create something actually useful here:
		 *  i.e. id column from 0 - 10000 to test MIN / MAX
		 */
		
		return null;
	}

	@Override
	protected void preSubmit() throws Exception {
		this.inputPath = createTempFile("input.txt", getInputString());
		this.resultPath = getTempFilePath("results");
	}

	// -------------------------------------------------------------------------------------------------------------
	// UDFs
	// -------------------------------------------------------------------------------------------------------------

	@SuppressWarnings("serial")
	public static final class Mapper extends MapFunction {

		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			// empty
		}
	}

	// -------------------------------------------------------------------------------------------------------------
	// Job vertex builder methods
	// -------------------------------------------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	private static InputFormatVertex createInput(JobGraph jobGraph, String pointsPath, int numSubTasks, TypeSerializerFactory<?> serializer) {
		CsvInputFormat pointsInFormat = new CsvInputFormat(' ', LongValue.class, LongValue.class, LongValue.class, LongValue.class);
		InputFormatVertex pointsInput = JobGraphUtils.createInput(pointsInFormat, pointsPath, "Input[Points]", jobGraph, numSubTasks);

		{
			TaskConfig taskConfig = new TaskConfig(pointsInput.getConfiguration());
			taskConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
			taskConfig.setOutputSerializer(serializer);
		}

		return pointsInput;
	}

	private static AbstractJobVertex createMapper(JobGraph jobGraph, int numSubTasks, TypeSerializerFactory<?> serializer) {
		AbstractJobVertex pointsInput = JobGraphUtils.createTask(RegularPactTask.class, "Map[DotProducts]", jobGraph, numSubTasks);

		{
			TaskConfig taskConfig = new TaskConfig(pointsInput.getConfiguration());

			taskConfig.setStubWrapper(new UserCodeClassWrapper<Mapper>(Mapper.class));
			taskConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
			taskConfig.setOutputSerializer(serializer);
			taskConfig.setDriver(CollectorMapDriver.class);
			taskConfig.setDriverStrategy(DriverStrategy.COLLECTOR_MAP);

			taskConfig.addInputToGroup(0);
			taskConfig.setInputLocalStrategy(0, LocalStrategy.NONE);
			taskConfig.setInputSerializer(serializer, 0);

			taskConfig.addOperatorStatistics(Record.class, new String[]{"f0", "f1"});
		}

		return pointsInput;
	}

	private static OutputFormatVertex createOutput(JobGraph jobGraph, String resultPath, int numSubTasks, TypeSerializerFactory<?> serializer) {
		OutputFormatVertex output = JobGraphUtils.createFileOutput(jobGraph, "Output", numSubTasks);

		{
			TaskConfig taskConfig = new TaskConfig(output.getConfiguration());
			taskConfig.addInputToGroup(0);
			taskConfig.setInputSerializer(serializer, 0);

			@SuppressWarnings("unchecked")
			CsvOutputFormat outFormat = new CsvOutputFormat("\n", " ", LongValue.class, LongValue.class, LongValue.class);
			outFormat.setOutputFilePath(new Path(resultPath));
			
			taskConfig.setStubWrapper(new UserCodeObjectWrapper<CsvOutputFormat>(outFormat));
		}

		return output;
	}

	// -------------------------------------------------------------------------------------------------------------
	// Unified solution set and workset tail update
	// -------------------------------------------------------------------------------------------------------------

	private JobGraph createJobGraphV1(String pointsPath, String centersPath, String resultPath, int numSubTasks) {

		
		
		// -- init -------------------------------------------------------------------------------------------------
		final TypeSerializerFactory<?> serializer = RecordSerializerFactory.get();

		JobGraph jobGraph = new JobGraph("Statistics Test Graph");

		// -- vertices ---------------------------------------------------------------------------------------------
		InputFormatVertex input = createInput(jobGraph, pointsPath, numSubTasks, serializer);
		AbstractJobVertex mapper = createMapper(jobGraph, numSubTasks, serializer);
		OutputFormatVertex output = createOutput(jobGraph, resultPath, numSubTasks, serializer);

		// -- edges ------------------------------------------------------------------------------------------------
		JobGraphUtils.connect(input, mapper, DistributionPattern.POINTWISE);
		JobGraphUtils.connect(mapper, output, DistributionPattern.POINTWISE);

		// -- instance sharing -------------------------------------------------------------------------------------
		
		SlotSharingGroup sharing = new SlotSharingGroup();
		
		input.setSlotSharingGroup(sharing);
		mapper.setSlotSharingGroup(sharing);
		output.setSlotSharingGroup(sharing);

		return jobGraph;
	}
}
