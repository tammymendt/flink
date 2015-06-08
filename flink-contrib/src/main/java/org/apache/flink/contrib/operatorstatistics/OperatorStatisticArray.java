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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;

public class OperatorStatisticArray implements Serializable {

	private ArrayList<OperatorStatistic> operatorStatistics;

	public OperatorStatisticArray(){
		operatorStatistics = new ArrayList<OperatorStatistic>();
		operatorStatistics.add(new CardinalityStatistic());
	}

	public OperatorStatisticArray(ArrayList<OperatorStatistic> opStats){
		operatorStatistics = new ArrayList<OperatorStatistic>(opStats);
	}

	public ArrayList<OperatorStatistic> getArray(){
		return operatorStatistics;
	}

	public int size(){
		return operatorStatistics.size();
	}

	public OperatorStatistic get(int index){
		return operatorStatistics.get(index);
	}

	public void add(OperatorStatistic opStat){
		operatorStatistics.add(opStat);
	}

	public void addAll(ArrayList<OperatorStatistic> opStats){
		for (OperatorStatistic opStat:opStats){
			if (statisticIndex(opStat.getType())<0){
				operatorStatistics.add(opStat);
			}
		}
	}

	public void addTuple(Object newTuple){
		for (OperatorStatistic opStat:operatorStatistics){
			opStat.add(newTuple);
		}
	}

	public void merge(OperatorStatisticArray other){
		for (int i=0;i<other.size();i++){
			int index = statisticIndex(other.get(i).getType());
			if (index < 0){
				operatorStatistics.add(other.get(i));
			}else{
				operatorStatistics.get(index).merge(other.get(i));
			}
		}
	}

	public int statisticIndex(OperatorStatistic.OperatorStatisticType opStatType){
		for (int i=0;i<operatorStatistics.size();i++){
			if (operatorStatistics.get(i).getType().equals(opStatType)){
				return i;
			}
		}
		return -1;
	}

	public String toString(){
		String out = "";
		for (OperatorStatistic opStat:operatorStatistics){
			out += opStat.toString();
		}
		return out;
	}

	public long getCardinality(){
		for (OperatorStatistic opStat: operatorStatistics){
			if (opStat.getType().equals(OperatorStatistic.OperatorStatisticType.CARDINALITY)){
				return ((CardinalityStatistic)opStat).getCardinality();
			}
		}
		return -1;
	}

	public Object getMin(){
		for (OperatorStatistic opStat: operatorStatistics){
			if (opStat.getType().equals(OperatorStatistic.OperatorStatisticType.MIN)){
				return ((MinStatistic)opStat).getMin();
			}
		}
		return null;
	}

	public Object getMax(){
		for (OperatorStatistic opStat: operatorStatistics){
			if (opStat.getType().equals(OperatorStatistic.OperatorStatisticType.MAX)){
				return ((MaxStatistic)opStat).getMax();
			}
		}
		return null;
	}

	public long getCountDistinctEstimate(){
		for (OperatorStatistic opStat: operatorStatistics){
			if (opStat.getType().equals(OperatorStatistic.OperatorStatisticType.COUNT_DISTINCT)){
				return ((CountDistinctStatistic)opStat).estimateCountDistinct();
			}
		}
		return -1;
	}

	public Map<Object,Long> getHeavyHitters(){
		for (OperatorStatistic opStat: operatorStatistics){
			if (opStat.getType().equals(OperatorStatistic.OperatorStatisticType.HEAVY_HITTER)){
				return ((HeavyHitterStatistic)opStat).getHeavyHitters();
			}
		}
		return null;
	}

}
