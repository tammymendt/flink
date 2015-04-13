package org.apache.flink.statistics.heavyhitters;

import org.junit.Test;

import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertTrue;

/**
 * Created by tamara on 08.04.15.
 */
public class LossyCountingTest {

    static final double fraction = 0.05;
    static final double error = 0.005;
    static final int seed = 7362181;
    static final Random r = new Random(seed);

    @Test
    public void testAccuracy() {

        int numItems = 1000000;
        long frequency = (int)Math.ceil(numItems* fraction);
        long minFrequency = (int)Math.ceil(numItems* (fraction-error));

        int[] xs = new int[numItems];
        int maxScale = 20;

        for (int i = 0; i < numItems; i++) {
            double p = r.nextDouble();
            if (p<0.2){
                xs[i] = r.nextInt(5);
            }else {
                int scale = r.nextInt(maxScale);
                xs[i] = r.nextInt(1 << scale);
            }
        }

        LossyCounting lossyCounting = new LossyCounting(fraction,error);

        for (int x : xs) {
            lossyCounting.addObject(x);
        }

        long[] actualFreq = new long[1 << maxScale];
        for (int x : xs) {
            actualFreq[x]++;
        }

        System.out.println("Size of heavy hitters: "+lossyCounting.heavyHitters.size());
        System.out.println(lossyCounting.toString());

/*        for (Map.Entry<Object, Long> entry : lossyCounting.getHeavyHitters().entrySet()){
            System.out.println("Heavy Hitter: "+entry.getKey()+": "+entry.getValue());
            System.out.println("True Frequency: "+actualFreq[(int)entry.getKey()]);
        }
*/
        for (int i = 0; i < actualFreq.length; ++i) {
            if (actualFreq[i]>=frequency) {
                assertTrue("All items with freq. > s.n will be found. Item " + i + ". Real freq. " + actualFreq[i]+" Expected freq."+frequency, lossyCounting.getHeavyHitters().containsKey(i));
            }
            if (lossyCounting.getHeavyHitters().containsKey(i)){
                assertTrue("no item with freq. < (s-e).n will be found. Item " + i + ". Real freq. " + actualFreq[i]+" Min freq."+ minFrequency, actualFreq[i]>=minFrequency);
                assertTrue("the estimated freq. underestimates the true freq. by < e.n. Real freq. " + actualFreq[i] + " Lower bound "+lossyCounting.getHeavyHitters().get(i),
                        Math.abs(lossyCounting.getHeavyHitters().get(i)-actualFreq[i]) < error*numItems);
            }
        }
    }

    @Test
    public void merge() throws HeavyHitterMergeException {
        int numToMerge = 5;
        int cardinality = 10000;
        int maxScale = 20;

        LossyCounting baseline = new LossyCounting(fraction,error);

        LossyCounting merged = new LossyCounting(fraction,error);

        LossyCounting[] sketches = new LossyCounting[numToMerge];
        for (int i = 0; i < numToMerge; i++) {
            sketches[i] = new LossyCounting(fraction,error/2);
            for (int j = 0; j < cardinality; j++) {
                double p = r.nextDouble();
                if (p<0.2){
                    int val = r.nextInt(5);
                    sketches[i].addObject(val);
                    baseline.addObject(val);
                }else {
                    int scale = r.nextInt(maxScale);
                    int val = r.nextInt(1 << scale);
                    sketches[i].addObject(val);
                    baseline.addObject(val);
                }
            }
            merged.merge(sketches[i]);
        }

        System.out.println("\nMERGED\n");
        System.out.println(merged.toString());

        System.out.println("\nBASELINE\n");
        System.out.println(baseline.toString());

        for (Map.Entry<Object, Long> entry : baseline.getHeavyHitters().entrySet()){
            assertTrue("Frequent item in baseline is not frequent in merged: " + entry.getKey(), merged.heavyHitters.containsKey(entry.getKey()));
        }
    }
}
