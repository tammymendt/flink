package org.apache.flink.examples.java.flatMap;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.OperatorStatsAccumulator;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.statistics.FieldStatistics;
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
        env.fromElements(1, 2, 34, 3).flatMap(new RichFlatMapFunction<Integer, Tuple1<Integer>>() {

            private OperatorStatsAccumulator accumulator;

            @Override
            public void open(Configuration parameters) {
                accumulator = getRuntimeContext().getOperatorStatsAccumulator("flatMapStats");
            }

            @Override
            public void flatMap(Integer value, Collector<Tuple1<Integer>> out) throws Exception {
                accumulator.add(value * 200);
                out.collect(new Tuple1(value * 200));
            }
        }).print();

        //.collectStatistics("flatMapStats", new String[]{"f0"})

        // execute program
        JobExecutionResult result = env.execute("Simple Flat Map Example");
        FieldStatistics fs = result.getAccumulatorResult("flatMapStats");
        System.out.println(fs.toString());
    }
}
