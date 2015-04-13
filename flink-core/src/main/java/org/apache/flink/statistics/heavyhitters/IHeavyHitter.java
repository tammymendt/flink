package org.apache.flink.statistics.heavyhitters;

import java.util.HashMap;

/**
 * Created by tamara on 23.02.15.
 */
public interface IHeavyHitter {

    void addObject(Object o);

    HashMap getHeavyHitters();

    long getTotalCardinality();

    void merge(IHeavyHitter toMerge) throws HeavyHitterMergeException;

}
