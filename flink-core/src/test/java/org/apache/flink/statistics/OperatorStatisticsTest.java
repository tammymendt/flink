package org.apache.flink.statistics;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import static org.junit.Assert.*;

public class OperatorStatisticsTest {

	public class Tuple{
		int f0;
		double f1;
		String f2;
		List<Integer> f3;

		public Tuple(){

		}

		public Tuple(int f0, double f1, String f2, List<Integer> f3){
			this.f0 = f0;
			this.f1 = f1;
			this.f2 = f2;
			this.f3 = f3;
		}
	}

	@Test
	public void testProcess() {

		//Initialize FieldStatsConfig and OperatorStatistics
		FieldStatisticsConfig[] fieldStatisticsConfigs = new FieldStatisticsConfig[4];
		for (int i=0;i<fieldStatisticsConfigs.length;i++){
			//Each fieldStatistics will be applied on a different field
			fieldStatisticsConfigs[i]= new FieldStatisticsConfig("f"+i);
		}
		OperatorStatistics operatorStatistics = new OperatorStatistics("testTask",fieldStatisticsConfigs);

		//Create dummy data with known distribution
		HashMap<Integer,Integer> f0Hash = new HashMap<Integer,Integer>();
		HashMap<Double,Integer> f1Hash = new HashMap<Double,Integer>();
		HashMap<String,Integer> f2Hash = new HashMap<String,Integer>();
		HashMap<List<Integer>,Integer> f3Hash = new HashMap<List<Integer>,Integer>();

		int seed = 7364181;
		String[] randomStrings = {"a","ab","abc","abcd"};
		Random r = new Random(seed);

		Tuple[] tuples = new Tuple[1000];
		for(int i=0;i<tuples.length;i++){
			double rD = r.nextDouble();
			tuples[i] = new Tuple();
			if (rD < 0.2){
				tuples[i].f0 = r.nextInt(1 << 4);
			}else{
				tuples[i].f0 = r.nextInt(1 << 10);
			}
			f0Hash.put(tuples[i].f0, 1);

			tuples[i].f1 = rD;
			f1Hash.put(tuples[i].f1, 1);

			tuples[i].f2 = randomStrings[r.nextInt(randomStrings.length-1)];
			f2Hash.put(tuples[i].f2, 1);

			List<Integer> intArray;
			intArray = new ArrayList<Integer>();
			intArray.add(r.nextInt(5));
			intArray.add(r.nextInt(5));
			intArray.add(r.nextInt(5));
			tuples[i].f3 = intArray;
			f3Hash.put(intArray, 1);

			operatorStatistics.process(tuples[i]);
		}

		assertEquals(operatorStatistics.totalCardinality, tuples.length);
		assertEquals(operatorStatistics.stats[0].countDistinct.cardinality(), f0Hash.size());
		assertEquals(operatorStatistics.stats[1].countDistinct.cardinality(), f1Hash.size());
		assertEquals(operatorStatistics.stats[2].countDistinct.cardinality(), f2Hash.size());
		assertEquals(operatorStatistics.stats[3].countDistinct.cardinality(), f3Hash.size());
	}
}
