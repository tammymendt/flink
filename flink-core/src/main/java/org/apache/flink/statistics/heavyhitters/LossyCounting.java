package org.apache.flink.statistics.heavyhitters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by tamara on 08.04.15.
 */
public class LossyCounting implements IHeavyHitter{

    double fraction;
    double error;
    int cardinality;
    Map<Object,Counter> heavyHitters;
    int bucket;

    private class Counter{
        long lowerBound;
        long frequencyError;

        private Counter(long lowerBound, long frequencyError){
            this.lowerBound = lowerBound;
            this.frequencyError = frequencyError;
        }

        private void updateLowerBound(int count){
            lowerBound+=count;
        }

        private long getUpperBound(){
            return lowerBound + frequencyError;
        }

    }

    public LossyCounting(double fraction, double error){

        this.fraction = fraction;
        this.error = error;
        this.cardinality = 0;
        this.heavyHitters = new HashMap<Object, Counter>();
        this.bucket = 0;
    }

    @Override
    public void addObject(Object o) {
        cardinality+=1;
        if (heavyHitters.containsKey(o)){
            heavyHitters.get(o).updateLowerBound(1);
        }else{
            heavyHitters.put(o,new Counter(1, bucket));
        }
        if (cardinality%(int)Math.ceil(1/error)==0) {
            bucket += 1;
            updateHeavyHitters();
        }
    }

    public void updateHeavyHitters(){
        ArrayList<Object> nonFrequentKeys = new ArrayList<Object>();
        for (Map.Entry<Object, Counter> entry : heavyHitters.entrySet()){
            if (entry.getValue().getUpperBound()< bucket){
                nonFrequentKeys.add(entry.getKey());
            }
        }
        for (Object o:nonFrequentKeys){
            heavyHitters.remove(o);
        }
    }

    public void merge(IHeavyHitter toMerge) throws HeavyHitterMergeException {
        try{
            LossyCounting lsToMerge = (LossyCounting)toMerge;
            if (this.fraction!=lsToMerge.fraction){
                throw new HeavyHitterMergeException("Both heavy hitter structures must be identical");
            }
            this.cardinality+=lsToMerge.cardinality;
            this.bucket = (int)Math.floor(cardinality*error);
            for (Map.Entry<Object, Counter> entry : lsToMerge.heavyHitters.entrySet()){
                Counter counter = this.heavyHitters.get(entry.getKey());
                if (counter==null){
                    this.heavyHitters.put(entry.getKey(),entry.getValue());
                }else{
                    Counter mergingCounter = entry.getValue();
                    this.heavyHitters.put(entry.getKey(),
                            new Counter(mergingCounter.lowerBound+counter.lowerBound, mergingCounter.frequencyError +counter.frequencyError));
                }
            }
            updateHeavyHitters();
        }catch (ClassCastException ex){
            throw new HeavyHitterMergeException("Both heavy hitter structures must be identical");
        }
    }

    @Override
    public HashMap<Object,Long> getHeavyHitters() {
        HashMap<Object,Long> heavyHitterLowerBounds = new HashMap<Object, Long>();
        long minFrequency = (long)Math.ceil(cardinality*(fraction-error));
        for (Map.Entry<Object, Counter> entry : heavyHitters.entrySet()){
            if(entry.getValue().lowerBound>=minFrequency){
                heavyHitterLowerBounds.put(entry.getKey(), entry.getValue().lowerBound);
            }
        }
        return heavyHitterLowerBounds;
    }

    @Override
    public long getTotalCardinality() {
        return cardinality;
    }

    @Override
    public String toString(){
        String out = "";
        long minFrequency = (long)Math.ceil(cardinality*(fraction-error));
        for (Map.Entry<Object, Counter> entry : heavyHitters.entrySet()){
            if(entry.getValue().lowerBound>=minFrequency) {
                out += entry.getKey().toString() + ": " + entry.getValue().lowerBound + " " + entry.getValue().frequencyError + "\n";
            }
        }
        return out;
    }
}
