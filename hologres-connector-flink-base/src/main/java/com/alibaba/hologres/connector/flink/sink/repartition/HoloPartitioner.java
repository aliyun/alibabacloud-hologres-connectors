package com.alibaba.hologres.connector.flink.sink.repartition;

import org.apache.flink.api.common.functions.Partitioner;

/** A {@link Partitioner} for Hologres. */
public class HoloPartitioner implements Partitioner<Integer> {
    // user can custom set sink parallelism, default null and will use numPartitions
    private final Integer parallelism;

    public HoloPartitioner(Integer parallelism) {
        this.parallelism = parallelism;
    }

    public int partition(Integer shardId, int numPartitions) {
        if (parallelism != null) {
            return shardId % parallelism;
        } else {
            return shardId % numPartitions;
        }
    }

    @Override
    public String toString() {
        return "HoloDistributionKeyPartitioner";
    }
}
