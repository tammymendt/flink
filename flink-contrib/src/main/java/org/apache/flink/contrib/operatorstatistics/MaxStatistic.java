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

public class MaxStatistic extends OperatorStatistic {

	private static final OperatorStatisticType OP_STAT_TYPE = OperatorStatisticType.MAX ;
	private Comparable max = null;

	public MaxStatistic() {}

	public Comparable getMax(){
		return max;
	}

	@Override
	public void add(Object value) {
		if ((max == null || ((Comparable) value).compareTo(max) > 0)) {
			max = (Comparable) value;
		}
	}

	@Override
	public void merge(OperatorStatistic other) {
		if (((MaxStatistic)other).getMax()!=null) {
			if (max == null || (this.max).compareTo(((MaxStatistic) other).getMax()) < 0) {
				this.max = ((MaxStatistic) other).getMax();
			}
		}
	}

	@Override
	public String toString(){
		if (max!=null){
			return "\nmax: " + max.toString();
		}else{
			return "\nmin: No max tracked ";
		}
	}

	@Override
	public OperatorStatisticType getType(){
		return OP_STAT_TYPE;
	}
}
