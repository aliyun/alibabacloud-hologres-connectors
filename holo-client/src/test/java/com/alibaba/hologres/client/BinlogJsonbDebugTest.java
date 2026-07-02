package com.alibaba.hologres.client;

import com.alibaba.hologres.client.impl.binlog.BinlogOffset;
import com.alibaba.hologres.client.model.HoloVersion;
import com.alibaba.hologres.client.model.OnConflictAction;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.model.binlog.BinlogRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Known server-side bug: jsonb + roaringbitmap in the same table causes "internal error: get binlog
 * next failed" when roaringbitmap column has non-null data. This test is skipped until the server
 * bug is fixed. To re-enable, change enabled=true.
 */
public class BinlogJsonbDebugTest extends HoloClientTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(BinlogJsonbDebugTest.class);

    @Test(enabled = false)
    public void testJsonbRoaringbitmapServerBug() throws Exception {
        HoloVersion needVersion = new HoloVersion(3, 3, 0);
        if (properties == null || holoVersion.compareTo(needVersion) < 0) {
            return;
        }

        try (Connection conn = buildConnection()) {
            String tableName = "holo_debug_jsonb_roaringbitmap_bug";

            execute(conn, new String[] {"drop table if exists " + tableName});
            execute(
                    conn,
                    new String[] {
                        "begin;",
                        "create table "
                                + tableName
                                + "(col_jsonb jsonb, col_rb roaringbitmap,"
                                + " pk int primary key)"
                                + " with (binlog_level='replica',table_group='tg_1')",
                        "commit;"
                    });

            try {
                HoloConfig insertConfig = buildConfig();
                insertConfig.setUseFixedFe(false);
                insertConfig.setOnConflictAction(OnConflictAction.INSERT_OR_REPLACE);
                try (HoloClient insertClient = new HoloClient(insertConfig)) {
                    TableSchema schema = insertClient.getTableSchema(tableName, true);
                    for (int i = 0; i < 2; i++) {
                        Record record = Record.build(schema);
                        record.setObject(0, "{\"a\":\"" + i + "\"}");
                        record.setObject(
                                1,
                                new byte[] {
                                    58, 48, 0, 0, 1, 0, 0, 0, 0, 0, 2, 0, 16, 0, 0, 0, 1, 0, 4, 0,
                                    5, 0
                                });
                        record.setObject(2, i);
                        insertClient.put(new Put(record));
                    }
                    insertClient.flush();
                }

                HoloConfig config = buildConfig();
                config.setUseFixedFe(false);
                config.setBinlogReadBatchSize(128);
                try (HoloClient client = new HoloClient(config)) {
                    Set<Integer> shards = IntStream.range(0, 1).boxed().collect(Collectors.toSet());
                    BinlogShardGroupReader reader =
                            client.binlogSubscribe(
                                    Subscribe.newOffsetBuilder(tableName)
                                            .addShardsStartOffset(
                                                    shards,
                                                    new BinlogOffset()
                                                            .setTimestamp("2021-04-12 12:12:12"))
                                            .build());
                    try {
                        int count = 0;
                        while (true) {
                            long timeout = (count < 2) ? 30000L : 3000L;
                            BinlogRecord record = reader.getBinlogRecord(timeout);
                            count++;
                            LOG.info(
                                    "Read record #{}: jsonb={}, rb={}",
                                    count,
                                    record.getObject(0),
                                    record.getObject(1));
                            if (count > 2) {
                                break;
                            }
                        }
                    } catch (TimeoutException e) {
                        LOG.info("Read completed with {} records", 2);
                    } finally {
                        reader.cancel();
                    }
                }
            } finally {
                execute(conn, new String[] {"drop table if exists " + tableName});
            }
        }
    }
}
