package org.apache.flink.statistics.heavyhitters;

import com.clearspring.analytics.hash.MurmurHash;
import com.clearspring.analytics.stream.frequency.CountMinSketch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Tamara on 2/18/2015.
 */
public class CountMinHeavyHitter implements IHeavyHitter {

    CountMinSketch countMinSketch;
    HashMap<Object,Long> heavyHitters;
    double fraction;
    long cardinality;

    public CountMinHeavyHitter(CountMinSketch countMinSketch, double fraction){
        this.countMinSketch = countMinSketch;
        this.cardinality = 0;
        this.fraction = fraction;
        this.heavyHitters = new HashMap<Object,Long>();
    }

    public void addLong(long item, long count) {
        countMinSketch.add(item,count);
        cardinality +=count;
        updateHeavyHitters(item);
    }

    public void addString(String item, long count) {
        countMinSketch.add(item,count);
        cardinality +=count;
        updateHeavyHitters(item);
    }

    @Override
    public void addObject(Object o) {
        long objectHash = MurmurHash.hash(o);
        countMinSketch.add(objectHash, 1);
        cardinality +=1;
        updateHeavyHitters(objectHash);
    }

    private void updateHeavyHitters(long item){
        long minFrequency = (long)Math.ceil(cardinality * fraction);
        long estimateCount = countMinSketch.estimateCount(item);
        if (estimateCount >= minFrequency){
            heavyHitters.put(item, estimateCount);
        }
        removeNonFrequent(minFrequency);
    }

    private void updateHeavyHitters(String item){
        long minFrequency = (long)Math.ceil(cardinality * fraction);
        long estimateCount = countMinSketch.estimateCount(item);
        if (estimateCount >= minFrequency){
            heavyHitters.put(item, estimateCount);
        }
        removeNonFrequent(minFrequency);
    }

    private void removeNonFrequent(long minFrequency){
        ArrayList<Object> nonFrequentKeys = new ArrayList<Object>();
        for (Map.Entry<Object, Long> entry : heavyHitters.entrySet()){
            if(entry.getValue()<minFrequency){
                nonFrequentKeys.add(entry.getKey());
            }
        }
        for (Object o:nonFrequentKeys){
            heavyHitters.remove(o);
        }
    }

    public void merge(IHeavyHitter toMerge) throws CMHeavyHitterMergeException {

        try {
            CountMinHeavyHitter cmToMerge = (CountMinHeavyHitter)toMerge;
            this.countMinSketch = CountMinSketch.merge(this.countMinSketch, cmToMerge.countMinSketch);

            if (this.fraction != cmToMerge.fraction) {
                throw new CMHeavyHitterMergeException("Frequency expectation cannot be merged");
            }

            for (Map.Entry<Object, Long> entry : cmToMerge.heavyHitters.entrySet()) {
                if (this.heavyHitters.containsKey(entry.getKey())){
                    this.heavyHitters.put(entry.getKey(),this.countMinSketch.estimateCount((Long)entry.getKey()));
                }else{
                    this.heavyHitters.put(entry.getKey(),entry.getValue());
                }
            }

            cardinality+=cmToMerge.cardinality;
            long minFrequency = (long) Math.ceil(cardinality * fraction);
            this.removeNonFrequent(minFrequency);

        }catch (ClassCastException ex){
            throw new CMHeavyHitterMergeException("Both heavy hitter objects must belong to the same class");
        }catch (Exception ex){
            throw new CMHeavyHitterMergeException("Cannot merge count min sketches: "+ex.getMessage());
        }
    }


    @Override
    public HashMap getHeavyHitters() {
        return heavyHitters;
    }

    @Override
    public long getTotalCardinality() {
        return cardinality;
    }


    protected static class CMHeavyHitterMergeException extends HeavyHitterMergeException {
        public CMHeavyHitterMergeException(String message) {
            super(message);
        }
    }

}
