package org.apache.flink.examples.java.flatMap;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.runtime.operators.shipping.StatisticsCollectorWrapper;
import org.apache.flink.util.Collector;

/**
 * Created by tamara on 26.03.15.
 */
public class FlatMap {

    // *************************************************************************
    //     PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(1,2,34,3).flatMap(new FlatMapFunction<Integer, Tuple1<Integer>>() {

            @Override
            public void flatMap(Integer value, Collector<Tuple1<Integer>> out) throws Exception {
                out.collect(new Tuple1(value*200));
            }
        }).collectKeyStatistics(new String[]{"f0"}).print();

        // execute program
        env.execute("Simple Flat Map Example");
    }
}
