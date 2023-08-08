package com.alibaba.hologres.hive.input;

import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.hive.conf.HoloClientParam;
import com.alibaba.hologres.hive.utils.JDBCUtils;
import com.alibaba.hologres.org.postgresql.jdbc.PgConnection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.alibaba.hologres.hive.utils.JDBCUtils.logErrorAndExceptionInConsole;

/** HoloRecordReader. */
public class HoloRecordReader implements RecordReader<LongWritable, MapWritable> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HoloRecordReader.class);

    TableSchema schema;

    private transient PgConnection conn;
    private transient PreparedStatement statement;
    private transient ResultSet resultSet;

    private int pos = 0;
    private HoloInputSplit inputSplit;

    public HoloRecordReader(TaskAttemptContext context, HoloInputSplit inputSplit)
            throws IOException {
        this.inputSplit = inputSplit;
        this.schema = inputSplit.getSchema();

        Configuration conf = context.getConfiguration();
        try {
            HoloClientParam param = new HoloClientParam(conf);

            String queryTemplate =
                    JDBCUtils.getSimpleSelectFromStatement(
                            schema.getTableName(), schema.getColumnSchema());
            String query =
                    String.format(
                            "%s WHERE hg_shard_id >= %s and hg_shard_id < %s",
                            queryTemplate, inputSplit.getStartShard(), inputSplit.getEndShard());
            String filterStr = conf.get(TableScanDesc.FILTER_TEXT_CONF_STR);
            if (filterStr != null && !filterStr.isEmpty()) {
                query = query + " and " + filterStr;
            }
            LOGGER.info("the bulk read query: {}", query);

            conn = JDBCUtils.createConnection(param).unwrap(PgConnection.class);
            conn.setAutoCommit(false);
            statement =
                    conn.prepareStatement(
                            query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            statement.setFetchSize(param.getScanFetchSize());
            statement.setQueryTimeout(param.getScanTimeoutSeconds());
            resultSet = statement.executeQuery();
        } catch (SQLException e) {
            close();
            logErrorAndExceptionInConsole("Error while create record reader.", e);
            throw new IOException(e);
        }
    }

    @Override
    public boolean next(LongWritable key, MapWritable value) throws IOException {
        try {
            if (resultSet.next()) {
                Column[] keys = schema.getColumnSchema();

                for (int i = 0; i < keys.length; i++) {
                    value.put(
                            new Text(keys[i].getName()), processValue(resultSet.getObject(i + 1)));
                }
                LOGGER.info("HoloRecordReader has more records to read.");
                key.set(pos);
                pos++;

                return true;
            } else {
                LOGGER.info("HoloRecordReader has no more records to read.");
                return false;
            }
        } catch (Exception e) {
            LOGGER.error("An error occurred while reading the next record from Hologres.", e);
            return false;
        }
    }

    @Override
    public LongWritable createKey() {
        return new LongWritable();
    }

    @Override
    public MapWritable createValue() {
        return new MapWritable();
    }

    @Override
    public long getPos() throws IOException {
        return pos;
    }

    private Writable processValue(Object value) {
        if (value == null) {
            return NullWritable.get();
        }
        return new Text(value.toString());
    }

    @Override
    public void close() throws IOException {
        if (conn != null) {
            try {
                conn.close();
                conn = null;
            } catch (SQLException e) {
                logErrorAndExceptionInConsole("Error while close record reader.", e);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public float getProgress() throws IOException {
        if (inputSplit == null) {
            return 0;
        } else {
            return inputSplit.getLength() > 0 ? pos / (float) inputSplit.getLength() : 1.0f;
        }
    }
}
