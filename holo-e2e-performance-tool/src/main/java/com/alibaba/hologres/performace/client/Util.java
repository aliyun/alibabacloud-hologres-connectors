package com.alibaba.hologres.performace.client;

import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class Util {
    public static Logger LOG = LoggerFactory.getLogger(Util.class);

    public static long[] calculateRowNumberPerJob(int jobCount, long rowNumber) {
        if (jobCount <= 0) {
            return null;
        }
        long[] rowNumberPerJob = new long[jobCount];
        long baseNumber = rowNumber / jobCount;
        long offset = rowNumber % jobCount;
        for (int i =0; i < jobCount; ++i) {
            if (i < offset) {
                rowNumberPerJob[i] = baseNumber + 1;
            } else {
                rowNumberPerJob[i] = baseNumber;
            }
        }
        return rowNumberPerJob;
    }

    public static List<String> getWriteColumnsName(PutTestConf conf, TableSchema schema) {
        List<String> columns = new ArrayList<>();
        if (conf.writeColumnCount > 0) {
            int columnCount = 0;
            for (int i = 0; i < schema.getColumnSchema().length && columnCount < conf.writeColumnCount; i++) {
                String columnName = schema.getColumn(i).getName();
                columns.add(columnName);
                if (!schema.isPrimaryKey(columnName) && columnName != "ts") {
                    columnCount++;
                }
            }
            if (conf.additionTsColumn) {
                columns.add("ts");
            }
        } else {
            columns.addAll(Arrays.stream(schema.getColumnSchema()).map(Column::getName).collect(Collectors.toList()));
        }
        return columns;
    }
}
