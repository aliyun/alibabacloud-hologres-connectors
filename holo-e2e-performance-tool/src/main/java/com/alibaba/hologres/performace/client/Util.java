package com.alibaba.hologres.performace.client;

import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.charset.StandardCharsets;
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

    public static String alignWithColumnSize(long value, int columnSize) {
        String val = String.valueOf(value);
        int len = val.length();
        if (len < columnSize) {
            int deltaLen = columnSize - len;
            StringBuilder sb = new StringBuilder();
            while (deltaLen-- > 0) {
                sb.append('0');
            }
            sb.append(val);
            return sb.toString();
        } else if (len > columnSize) {
            return val.substring(0, columnSize);
        } else {
            return val;
        }
    }

    private static float parseFloat(String str) {
        try {
            return Float.parseFloat(str);
        } catch (NumberFormatException e) {
            return 0.0f;
        }
    }

    public static long getMemoryStat() {
        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
        String pid = runtime.getName().split("@")[0];
        long oldGen = 0, permGen = 0, edenSpace = 0, survivorSpace = 0, metacSpace = 0;
        Process process = null;
        try {
            process = Runtime.getRuntime().exec("jstat -gc " + pid);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))){
                String line = reader.readLine();
                LOG.info("{}", line);
                String[] headers = line.trim().split("\\s+");
                line = reader.readLine();
                LOG.info("{}", line);
                String[] values = line.trim().split("\\s+");
                for (int i =0; i<headers.length; i++) {
                    if (headers[i].equalsIgnoreCase("S0U")) {
                        survivorSpace = (long)parseFloat(values[i]);
                    } else if (headers[i].equalsIgnoreCase("S1U")) {
                        survivorSpace += (long)parseFloat(values[i]);
                    } else if (headers[i].equalsIgnoreCase("EU")) {
                        edenSpace += (long)parseFloat(values[i]);
                    } else if (headers[i].equalsIgnoreCase("OU")) {
                        oldGen += (long)parseFloat(values[i]);
                    } else if (headers[i].equalsIgnoreCase("PU")) {
                        permGen += (long)parseFloat(values[i]);
                    } else if (headers[i].equalsIgnoreCase("MU")) {
                        metacSpace += (long)parseFloat(values[i]);
                    }
                }
            }
        } catch (IOException ioException) {
            ioException.printStackTrace();
        } finally {
            if (process!= null) {
                process.destroy();
            }
        }

        // 计算总内存使用量
        long totalMemory = edenSpace + survivorSpace + oldGen + permGen + metacSpace;
        LOG.info("Eden Space: {} KB", edenSpace);
        LOG.info("Survivor Space: {} KB", survivorSpace);
        LOG.info("Old Generation: {} KB", oldGen);
        LOG.info("Perm Generation: {} KB", permGen);
        LOG.info("Meta Space: {} KB", metacSpace);
        LOG.info("Total Memory: {} KB", totalMemory);
        return totalMemory;
    }

    public static void dumpHeap(String confName) {
        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
        String pid = runtime.getName().split("@")[0];
        Process process = null;
        File dir = new File(confName).getParentFile();
        File file = new File(dir, "heapdump.phrof");
        String cmd = "jmap -dump:format=b,file=" + file.getPath() + " " + pid;
        LOG.info("dump memory cmd: {}", cmd);
        try {
            process = Runtime.getRuntime().exec(cmd);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    LOG.info("{}", line);
                }
            }
            } catch (IOException ioException) {
            ioException.printStackTrace();
        } finally {
            if (process!= null) {
                process.destroy();
            }
        }
    }
}
