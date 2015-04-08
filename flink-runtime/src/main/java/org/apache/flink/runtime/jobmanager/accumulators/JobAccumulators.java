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


package org.apache.flink.runtime.jobmanager.accumulators;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.runtime.jobgraph.JobVertexID;

/**
 * Simple class wrapping a map of accumulators for a single job. Just for better
 * handling.
 */
public class JobAccumulators {

	private final Map<String, Accumulator<?, ?>> accumulators = new HashMap<String, Accumulator<?, ?>>();
    private final Map<String,Map<String, Accumulator<?,?>>> taskAccumulators = new HashMap<String,Map<String,Accumulator<?,?>>>();

	public Map<String, Accumulator<?, ?>> getAccumulators() {
		return this.accumulators;
	}

    public Map<String, Map<String,Accumulator<?, ?>>> getTaskAccumulators() {
        return this.taskAccumulators;
    }

	public void processNew(int taskIndex, Map<String, Accumulator<?, ?>> newAccumulators) {
		AccumulatorHelper.mergeInto(this.accumulators, newAccumulators);
        //TODO this next step should be optional
        AccumulatorHelper.mergeInto(this.taskAccumulators, String.valueOf(taskIndex), newAccumulators);
	}
}
