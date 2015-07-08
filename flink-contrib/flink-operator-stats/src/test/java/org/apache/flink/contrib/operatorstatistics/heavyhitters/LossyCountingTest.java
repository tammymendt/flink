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

import org.apache.commons.math3.distribution.ParetoDistribution;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertTrue;

/*
* Test the structure implemented for Lossy Counting
*/

public class LossyCountingTest {

	private static final Logger LOG = LoggerFactory.getLogger(LossyCountingTest.class);

	static final double fraction = 0.01;
	static final double error = 0.005;
	static final ParetoDistribution p = new ParetoDistribution();
	static final int cardinality = 1000000;
	static final int maxScale = 100000;
	static final long frequency = (int)Math.ceil(cardinality* fraction);
	static final long minFrequency = (int)Math.ceil(cardinality* (fraction-error));

	@Test
	public void testAccuracy() {

		long[] actualFreq = new long[maxScale];

		LossyCounting lossyCounting = new LossyCounting(fraction,error);

		for (int i = 0; i < cardinality; i++) {
			int value = (int)Math.round(p.sample())%maxScale;
			lossyCounting.addObject(value);
			actualFreq[value]++;
		}

		LOG.debug("Size of heavy hitters: "+lossyCounting.getHeavyHitters().size());
		LOG.debug(lossyCounting.toString());

/*		Iterator it = lossyCounting.getHeavyHitters().entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry heavyHitter = (Map.Entry)it.next();
			System.out.println(heavyHitter.getKey() + " -> " + heavyHitter.getValue());
		}*/

		for (int i = 0; i < actualFreq.length; ++i) {
			if (actualFreq[i]>=frequency) {
				assertTrue("All items with freq. > s.n will be found. Item " + i + ". Real freq. " + actualFreq[i]+" Expected freq."+frequency, lossyCounting.getHeavyHitters().containsKey(i));
			}
			if (lossyCounting.getHeavyHitters().containsKey(i)){
				assertTrue("no item with freq. < (s-e).n will be found. Item " + i + ". Real freq. " + actualFreq[i]+" Min freq."+ minFrequency, actualFreq[i]>=minFrequency);
				assertTrue("the estimated freq. underestimates the true freq. by < e.n. Real freq. " + actualFreq[i] + " Lower bound "+lossyCounting.getHeavyHitters().get(i),
						Math.abs(lossyCounting.getHeavyHitters().get(i)-actualFreq[i]) < error*cardinality);
			}
		}
	}

	@Test
	public void merge() throws HeavyHitterMergeException {
		int numToMerge = 5;

		LossyCounting baseline = new LossyCounting(fraction,error);
		LossyCounting merged = new LossyCounting(fraction,error);
		LossyCounting[] sketches = new LossyCounting[numToMerge];

		long[] actualFreq = new long[maxScale];

		for (int i = 0; i < numToMerge; i++) {
			sketches[i] = new LossyCounting(error*2,error);
			for (int j = 0; j < cardinality; j++) {
				int val = (int)Math.round(p.sample())%10000;
				sketches[i].addObject(val);
				baseline.addObject(val);
				actualFreq[val]++;
			}
			merged.merge(sketches[i]);
		}

		System.out.println("\nMERGED\n" + merged.toString());
		System.out.println("\nBASELINE\n" + baseline.toString());

		for (Map.Entry<Object, Long> entry : baseline.getHeavyHitters().entrySet()){
			assertTrue("Frequent item in baseline is not detected in merged: " + entry.getKey(), merged.getHeavyHitters().containsKey(entry.getKey()));
		}

		for (int i = 0; i < actualFreq.length; ++i) {
			if (actualFreq[i]>=frequency) {
				assertTrue("All items with freq. > s.n will be found. Item " + i + ". Real freq. " + actualFreq[i]+" Expected freq."+frequency, merged.getHeavyHitters().containsKey(i));
			}
			if (merged.getHeavyHitters().containsKey(i)){
				assertTrue("no item with freq. < (s-e).n will be found. Item " + i + ". Real freq. " + actualFreq[i]+" Min freq."+ minFrequency, actualFreq[i]>=minFrequency);
				assertTrue("the estimated freq. underestimates the true freq. by < e.n. Real freq. " + actualFreq[i] + " Lower bound "+merged.getHeavyHitters().get(i),
						Math.abs(merged.getHeavyHitters().get(i)-actualFreq[i]) < error*cardinality);
			}
		}

	}
}
