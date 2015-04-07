package org.apache.flink.api.common.accumulators;

import org.apache.flink.statistics.FieldStatistics;
import org.apache.flink.statistics.FieldStatisticsConfig;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Created by tamara on 07.04.15.
 */
public class OperatorStatsAccumulator implements Accumulator<Object, FieldStatistics> {

    private FieldStatistics local;

    public OperatorStatsAccumulator(){
        local = new FieldStatistics(new FieldStatisticsConfig(""));
    }

    @Override
    public void add(Object value) {
        local.process(value);
    }

    @Override
    public FieldStatistics getLocalValue() {
        return local;
    }

    @Override
    public void resetLocal() {
        local = new FieldStatistics(new FieldStatisticsConfig(""));
    }

    @Override
    public void merge(Accumulator<Object, FieldStatistics> other) {
        local.merge(other.getLocalValue());
    }

    @Override
    public void write(ObjectOutputStream out) throws IOException {
        out.writeObject(local);
    }

    @Override
    public void read(ObjectInputStream in) throws IOException {
        try {
            local = (FieldStatistics) in.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Accumulator<Object, FieldStatistics> clone() {
        OperatorStatsAccumulator stats = new OperatorStatsAccumulator();
        stats.local = this.local;
        return stats;
    }
}
