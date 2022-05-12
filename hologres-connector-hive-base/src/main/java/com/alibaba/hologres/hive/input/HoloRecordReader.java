package com.alibaba.hologres.hive.input;

import com.alibaba.hologres.client.Exporter;
import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.RecordInputFormat;
import com.alibaba.hologres.client.Scan;
import com.alibaba.hologres.client.SortKeys;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.ExportContext;
import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.RecordScanner;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.hive.HoloClientProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

/** HoloRecordReader. */
public class HoloRecordReader implements RecordReader<LongWritable, MapWritable> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HoloRecordReader.class);

    HoloClientProvider clientProvider;
    HoloClient client;
    RecordScanner scanner;
    TableSchema schema;
    RecordInputFormat recordFormat;
    boolean isScan = false;

    private int pos = 0;
    private HoloInputSplit inputSplit;

    public HoloRecordReader(TaskAttemptContext context, HoloInputSplit inputSplit)
            throws IOException {
        this.inputSplit = inputSplit;

        Configuration conf = context.getConfiguration();
        try {
            clientProvider = new HoloClientProvider(conf);
            client = clientProvider.createOrGetClient();
            schema = clientProvider.getTableSchema();

            String filterXml = conf.get(TableScanDesc.FILTER_EXPR_CONF_STR);
            // Use copy only when copyMode is true and there is no filter EXPR
            if (filterXml == null && clientProvider.isCopyMode()) {
                LOGGER.info("Use copy mode");

                ExportContext er = client.exportData(Exporter.newBuilder(schema).build());
                recordFormat = new RecordInputFormat(er, schema);
            } else {
                // Use scan default, or have filter conditions
                LOGGER.info("Use scan mode");

                Scan.Builder scanBuilder = Scan.newBuilder(schema);
                scanner = client.scan(scanBuilder.setSortKeys(SortKeys.NONE).build());
                ExprNodeDesc conditionNode =
                        SerializationUtilities.deserializeExpression(filterXml);
                walk(conditionNode, scanBuilder);

                isScan = true;
            }
        } catch (HoloClientException e) {
            close();
            throw new IOException(e);
        }
    }

    @Override
    public boolean next(LongWritable key, MapWritable value) throws IOException {
        if (isScan) {
            return scanNext(key, value);
        } else {
            return copyNext(key, value);
        }
    }

    private boolean scanNext(LongWritable key, MapWritable value) {
        try {
            if (scanner.next()) {
                Record record = scanner.getRecord();
                Column[] keys = record.getSchema().getColumnSchema();
                Object[] values = record.getValues();

                for (int i = 0; i < values.length; i++) {
                    value.put(new Text(keys[i].getName()), processValue(values[i]));
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

    private boolean copyNext(LongWritable key, MapWritable value) {
        Record record;
        try {
            if ((record = recordFormat.getRecord()) != null) {
                Column[] keys = record.getSchema().getColumnSchema();
                Object[] values = record.getValues();

                for (int i = 0; i < values.length; i++) {
                    value.put(new Text(keys[i].getName()), processValue(values[i]));
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
        if (client != null) {
            try {
                client.flush();
            } catch (HoloClientException throwables) {
                throw new IOException(throwables);
            }
            client.close();
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

    Stack<ExprNodeDesc> opStack = new Stack<>();
    List<ExprNodeDesc> walkList = new ArrayList();

    /** 将filter条件传递给holo，提高scan效率（hive在本地也会进行filter）. */
    public void walk(ExprNodeDesc conditionNode, Scan.Builder scanBuilder) {

        opStack.push(conditionNode);
        walkList.add(conditionNode);

        while (!opStack.empty()) {
            ExprNodeDesc node = opStack.pop();

            if (node.toString().startsWith("GenericUDFOPOr")) {
                LOGGER.warn("Not Support OR Filter Currently : " + node.getExprString());
                continue;
            }
            if (node.getChildren().get(0).getChildren() == null) {
                LOGGER.info("Add Filter: " + node.getExprString());

                if (node.toString().startsWith("GenericUDFOPEqual")) {
                    scanBuilder.addEqualFilter(
                            node.getChildren().get(0).getExprString(),
                            node.getChildren().get(1).getExprString());
                } else if (node.toString().startsWith("GenericUDFOPGreaterThan")) {
                    scanBuilder.addRangeFilter(
                            node.getChildren().get(0).getExprString(),
                            node.getChildren().get(1).getExprString(),
                            null);
                } else if (node.toString().startsWith("GenericUDFOPLessThan")) {
                    scanBuilder.addRangeFilter(
                            node.getChildren().get(0).getExprString(),
                            null,
                            node.getChildren().get(1).getExprString());
                } else if (node.toString().startsWith("GenericUDFBetween")) {
                    scanBuilder.addRangeFilter(
                            node.getChildren().get(1).getExprString(),
                            node.getChildren().get(2).getExprString(),
                            node.getChildren().get(3).getExprString());
                } else {
                    continue;
                }
                walkList.add(node);
            } else {
                for (ExprNodeDesc childNode : node.getChildren()) {
                    if (!walkList.contains(childNode)) {
                        opStack.push(childNode);
                    }
                }
            }
        }
    }
}
