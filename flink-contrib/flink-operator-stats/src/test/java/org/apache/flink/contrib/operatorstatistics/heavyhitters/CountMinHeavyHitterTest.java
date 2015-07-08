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

package org.apache.flink.contrib.operatorstatistics.heavyhitters;

import com.clearspring.analytics.stream.frequency.CountMinSketch;
import org.apache.commons.math3.distribution.ParetoDistribution;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 * Test the structure built to track heavy hitters using the count min sketch from
 * {@link com.clearspring.analytics.stream.frequency.CountMinSketch}
 */
public class CountMinHeavyHitterTest {

	static final double fraction = 0.01;
	static final double error = 0.005;
	static final double confidence = 0.99;
	static final int seed = 7362181;
	static final ParetoDistribution p = new ParetoDistribution();
	static final int cardinality = 1000000;
	static final int maxScale = 100000;
	static final long frequency = (int)Math.ceil(cardinality* fraction);

	@Test
	public void testAccuracy() {

		long[] actualFreq = new long[maxScale];

		CountMinHeavyHitter cmTopK = new CountMinHeavyHitter(fraction,error,confidence,seed);

		for (int i = 0; i < cardinality; i++) {
			int value = (int)Math.round(p.sample())%maxScale;
			cmTopK.addObject(value);
			actualFreq[value]++;
		}

/*		System.out.println("Min expected frequency: "+frequency);
		System.out.println("Found Heavy Hitters: ");
		Iterator it = cmTopK.getHeavyHitters().entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry heavyHitter = (Map.Entry)it.next();
			System.out.println(heavyHitter.getKey() + " -> " + heavyHitter.getValue() + " -> "+actualFreq[(Integer)heavyHitter.getKey()]);
		}*/

		for (int i=0;i<actualFreq.length;i++){
			if (actualFreq[i]>frequency){
				assertTrue("Heavy Hitter not found :" + i +","+ actualFreq[i], cmTopK.getHeavyHitters().containsKey(i));
			}
		}

		Iterator it = cmTopK.getHeavyHitters().entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry heavyHitter = (Map.Entry)it.next();
			Long estimateError = (Long)heavyHitter.getValue() - actualFreq[(Integer)heavyHitter.getKey()];
			assertTrue("Difference between real frequency and estimate is too large: " + estimateError,
					estimateError < (error*cardinality));
		}
	}

	@Test
	public void merge() throws CountMinHeavyHitter.CMHeavyHitterMergeException {

		int numToMerge = 5;

		CountMinSketch sketchBaseline = new CountMinSketch(error, confidence, seed);
		CountMinHeavyHitter baseline = new CountMinHeavyHitter(sketchBaseline,fraction);
		CountMinHeavyHitter merged = null;

		CountMinHeavyHitter[] sketches = new CountMinHeavyHitter[numToMerge];
		for (int i = 0; i < numToMerge; i++) {
			CountMinSketch cms = new CountMinSketch(error, confidence, seed);
			sketches[i] = new CountMinHeavyHitter(cms, fraction);
			for (int j = 0; j < cardinality; j++) {
				int val = (int)Math.round(p.sample())%maxScale;
				sketches[i].addObject(val);
				baseline.addObject(val);
			}
			if (i==0){
				merged = sketches[0];
			}else{
				merged.merge(sketches[i]);
			}
		}

		System.out.println("\nMERGED\n" + merged.toString());
		System.out.println("\nBASELINE\n" + baseline.toString());

		for (Map.Entry<Object, Long> entry : baseline.getHeavyHitters().entrySet()){
			assertTrue("Frequent item in baseline is not frequent in merged: " + entry.getKey(), merged.getHeavyHitters().containsKey(entry.getKey()));
		}

	}

}
