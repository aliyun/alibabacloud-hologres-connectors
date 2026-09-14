package com.alibaba.hologres.connector.flink.sink.repartition;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;

import com.alibaba.hologres.client.impl.util.ShardUtil;
import com.alibaba.hologres.org.postgresql.jdbc.TimestampUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * RowDataShardUtil.
 *
 * <p>Based on a row of data and the key index (Distribution Key) to be hashed, and return the hash
 * result, which is consistent with the distribution hash algorithm of hologres.
 */
public class RowDataShardUtil extends ShardUtil {
    /*
     * hologres currently only supports the following types as distribution keys.
     *  1. Integer types: smallint, int, bigint
     *  2. String types: text, varchar, char
     *  3. Time types: date, timestamp
     *      Among them, date type is stored as int in holo, and timestamp is stored as microsecond long in holo
     *  4. Boolean type: boolean
     */
    public static int hash(RowData row, Integer[] indexes, LogicalType[] fieldTypes) {
        int hash = 0;
        boolean first = true;
        if (indexes == null || indexes.length == 0) {
            ThreadLocalRandom rand = ThreadLocalRandom.current();
            hash = rand.nextInt();
        } else {
            for (int i : indexes) {
                Object o;
                LogicalType type = fieldTypes[i];
                switch (type.getTypeRoot()) {
                    case BINARY:
                    case VARBINARY:
                        o = row.getBinary(i);
                        break;
                    case CHAR:
                    case VARCHAR:
                        o = row.getString(i).toString();
                        break;
                    case TINYINT:
                        o = row.getByte(i);
                        break;
                    case SMALLINT:
                        o = row.getShort(i);
                        break;
                    case INTEGER:
                    case DATE:
                        o = row.getInt(i);
                        break;
                    case BIGINT:
                        o = row.getLong(i);
                        break;
                    case BOOLEAN:
                        o = row.getBoolean(i);
                        break;
                    case TIMESTAMP_WITHOUT_TIME_ZONE:
                        o =
                                TimestampUtil.timestampToMicroSecond(
                                        row.getTimestamp(i, ((TimestampType) type).getPrecision())
                                                .toTimestamp(),
                                        "timestamp",
                                        false);
                        break;
                    case TIME_WITHOUT_TIME_ZONE:
                    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                        throw new IllegalArgumentException(
                                String.format(
                                        "Hologres sink does not support %s as distribution key",
                                        type.getTypeRoot()));
                    default:
                        throw new IllegalArgumentException(
                                String.format(
                                        "Hologres not support %s type be distribution key",
                                        type.getTypeRoot()));
                }
                if (first) {
                    hash = ShardUtil.hash(o);
                } else {
                    hash ^= ShardUtil.hash(o);
                }
                first = false;
            }
        }
        return hash;
    }

    /** Computes the Hologres shard id for a row using the table distribution keys. */
    public static int getShardId(
            RowData row, Integer[] indexes, LogicalType[] fieldTypes, int shardCount) {
        int raw = hash(row, indexes, fieldTypes);
        int hash = Integer.remainderUnsigned(raw, RANGE_END);
        int[][] range = split(shardCount);
        int low = 0;
        int high = shardCount - 1;
        while (low <= high) {
            int middle = (low + high) >>> 1;
            if (hash < range[middle][0]) {
                high = middle - 1;
            } else if (hash >= range[middle][1]) {
                low = middle + 1;
            } else {
                return middle;
            }
        }
        throw new IllegalStateException("Failed to route hash " + hash + " to a Hologres shard");
    }

    /**
     * Returns the complete shard group assigned to the partition containing {@code routedShard}.
     */
    public static List<Integer> getTargetShardList(
            int routedShard, int shardCount, int parallelism) {
        if (routedShard < 0 || routedShard >= shardCount) {
            throw new IllegalArgumentException(
                    "Invalid routed shard " + routedShard + " for shard count " + shardCount);
        }
        if (parallelism <= 0) {
            throw new IllegalArgumentException("Invalid sink parallelism " + parallelism);
        }
        List<Integer> shards = new ArrayList<>();
        int partition = routedShard % parallelism;
        for (int shardId = partition; shardId < shardCount; shardId += parallelism) {
            shards.add(shardId);
        }
        return Collections.unmodifiableList(shards);
    }
}
