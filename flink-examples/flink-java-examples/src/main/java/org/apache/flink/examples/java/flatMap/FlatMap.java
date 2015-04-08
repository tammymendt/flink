package org.apache.flink.examples.java.flatMap;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.OperatorStatsAccumulator;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.record.io.FileOutputFormat;
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
        env.readTextFile("/home/tamara/Documents/Thesis/data-generation/input.txt").
                flatMap(new RichFlatMapFunction<String, Tuple1<Integer>>() {

                    private OperatorStatsAccumulator accumulator;

                    @Override
                    public void open(Configuration parameters) {
                        accumulator = getRuntimeContext().getOperatorStatsAccumulator("flatMapStats");
                    }

                    @Override
                    public void flatMap(String value, Collector<Tuple1<Integer>> out) throws Exception {
                        int intValue;
                        try {
                            intValue = Integer.parseInt(value);
                            accumulator.add( intValue);
                            out.collect(new Tuple1(intValue));
                        }catch(NumberFormatException ex){}
                    }
                }).writeAsCsv("/home/tamara/Documents/Thesis/data-generation","\n",",");

                    //.collectStatistics("flatMapStats", new String[]{"f0"})

                    // execute program
                    JobExecutionResult result = env.execute("Simple Flat Map Example");
                    FieldStatistics fs = result.getAccumulatorResult("flatMapStats");
                    System.out.println(fs.toString());
    }
}
