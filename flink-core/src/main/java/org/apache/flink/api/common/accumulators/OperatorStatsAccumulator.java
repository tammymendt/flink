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


    private FieldStatistics[] local;
    private int local_index;
    private FieldStatistics global;

    public OperatorStatsAccumulator(int subTaskIndex, int numSubtasks){
        global = new FieldStatistics(new FieldStatisticsConfig(""));
        local = new FieldStatistics[numSubtasks];
        local_index = subTaskIndex;
        local[local_index] = new FieldStatistics(new FieldStatisticsConfig(""));
    }

    @Override
    public void add(Object value) {
        local[local_index].process(value);
        global.process(value);
    }

    @Override
    public FieldStatistics getLocalValue() {
        return local[local_index];
    }

    @Override
    public void resetLocal() {
        local[local_index] = new FieldStatistics(new FieldStatisticsConfig(""));
    }

    @Override
    public void merge(Accumulator<Object, FieldStatistics> other) {
        local[((OperatorStatsAccumulator)other).getLocal_index()] = other.getLocalValue();
        global.merge(other.getLocalValue());
    }

    @Override
    public void write(ObjectOutputStream out) throws IOException {
        out.writeObject(local[local_index]);
    }

    @Override
    public void read(ObjectInputStream in) throws IOException {
        try {
            local = (FieldStatistics[]) in.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Accumulator<Object, FieldStatistics> clone() {
        OperatorStatsAccumulator stats = new OperatorStatsAccumulator(this.local_index,this.local.length);
        stats.global = this.global;
        stats.local = new FieldStatistics[this.local.length];
        for (int i=0;i<stats.local.length;i++){
            stats.local[i] = this.local[i];
        }
        return stats;
    }

    public int getLocal_index(){
        return local_index;
    }

    public FieldStatistics[] getLocalStatistics(){
        return this.local;
    }

    public FieldStatistics getGlobalStatistics(){
        return this.global;
    }
}
