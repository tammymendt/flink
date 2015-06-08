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

public class MinStatistic extends OperatorStatistic {

	private static final OperatorStatisticType OP_STAT_TYPE = OperatorStatisticType.MIN ;
	private Comparable min = null;

	public MinStatistic(){}

	public Comparable getMin(){
		return min;
	}

	@Override
	public void add(Object tupleObject) {
		if ((min == null || ((Comparable)tupleObject).compareTo(min) < 0)) {
			min = (Comparable)tupleObject;
		}
	}

	@Override
	public void merge(OperatorStatistic other){
		if (((MinStatistic)other).getMin()!=null) {
			if (min == null || min.compareTo(((MinStatistic) other).getMin()) > 0) {
				this.min = ((MinStatistic) other).getMin();
			}
		}
	}

	@Override
	public String toString(){
		if (min!=null){
			return "\nmin: " + min.toString();
		}else{
			return "\nmin: No min tracked ";
		}
	}

	@Override
	public OperatorStatisticType getType(){
		return OP_STAT_TYPE;
	}
}
