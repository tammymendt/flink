package org.apache.flink.examples.java.flatMap;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.OperatorStatsAccumulator;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.AggregationFunction;
import org.apache.flink.api.java.record.io.FileOutputFormat;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
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
        env.readTextFile("/home/tamara/Documents/Thesis/data-generation/input.txt").
                flatMap(new RichFlatMapFunction<String, Tuple2<Integer, Integer>>() {

                    private OperatorStatsAccumulator intAccumulator;

                    @Override
                    public void open(Configuration parameters) {
                        intAccumulator = getRuntimeContext().getOperatorStatsAccumulator("intAccumulator");
                    }

                    @Override
                    public void flatMap(String value, Collector<Tuple2<Integer, Integer>> out) throws Exception {
                        int intValue;
                        try {
                            intValue = Integer.parseInt(value);
                            intAccumulator.add(intValue);
                            out.collect(new Tuple2(intValue % 5, intValue));
                        } catch (NumberFormatException ex) {
                        }
                    }
                }).writeAsCsv("/home/tamara/Documents/Thesis/data-generation", "\n", ",");

                    //.collectStatistics("flatMapStats", new String[]{"f0"})

        // execute program
        JobExecutionResult result = env.execute("Simple Flat Map Example");
        OperatorStatsAccumulator op = result.getAccumulatorResult("intAccumulator");
        System.out.println("Global Stats");
        System.out.println(op.getGlobalStatistics().toString());
        for (int i=0;i<op.getLocalStatistics().length;i++){
            System.out.println("Local Stats " + i);
            System.out.println(op.getLocalStatistics()[i].toString());
        }
    }
}
