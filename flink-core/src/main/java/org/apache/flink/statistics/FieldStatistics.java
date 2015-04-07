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

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.clearspring.analytics.stream.cardinality.LinearCounting;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Gathers statistical information of a given field in a Tuple that has been emitted via a {@link org.apache.flink.util.Collector}.
 * The statistics that wish to be gathered for a given field are configurable and represented by a
 * {@link org.apache.flink.statistics.FieldStatisticsConfig} object.
 * This information is expressed in the config Object of a TaskConfig
 *
 * Information that can be gathered includes min value, max value, count distinct (approximation), frequency (approximation), top-k
 *
 */
public class FieldStatistics implements Serializable {

	FieldStatisticsConfig config;

    Object min;
	Object max;
	ICardinality countDistinct;

	public FieldStatistics(FieldStatisticsConfig config) {
		this.config = config;
		if (config.countDistinctAlgorithm.equals(FieldStatisticsConfig.CountDistinctAlgorithm.LINEAR_COUNTING)) {
			countDistinct = new LinearCounting(OperatorStatistics.COUNTD_BITMAP_SIZE);
		}
		if(config.countDistinctAlgorithm.equals(FieldStatisticsConfig.CountDistinctAlgorithm.HYPERLOGLOG)){
			countDistinct = new HyperLogLog(OperatorStatistics.COUNTD_LOG2M);
		}
	}

	public void process(Object tupleObject){
		if (tupleObject instanceof Comparable) {
			if (config.collectMin && (min == null || ((Comparable) tupleObject).compareTo(min) < 0)) {
				min = tupleObject;
			}
			if (config.collectMax && (max == null || ((Comparable) tupleObject).compareTo(max) > 0)) {
				max = tupleObject;
			}
		}
		if (config.collectCountDistinct){
			countDistinct.offer(tupleObject);
		}
	}

    public void merge(FieldStatistics other){

        if (this.config.collectMin && ((Comparable)this.min).compareTo(other.min)<0 ) {
            this.min = other.min;
        }
        if (this.config.collectMax && ((Comparable)this.max).compareTo(other.max)>0 ) {
            this.max = other.max;
        }

        try {
            ICardinality mergedCountDistinct = this.countDistinct.merge(new ICardinality[]{this.countDistinct,other.countDistinct});
            this.countDistinct = mergedCountDistinct;
        } catch (CardinalityMergeException e) {
            e.printStackTrace();
        }
    }

    public Object getMin() {
        return min;
    }

    public Object getMax() {
        return max;
    }

    public long estimateCountDistinct(){
        return countDistinct.cardinality();
    }

    @Override
    public String toString(){
        String out = "\nmax: "+this.max;
        out+="\nmin: "+this.min;
        out+="\ncount distinct estimate: "+this.countDistinct.cardinality();
        return out;
    }

}