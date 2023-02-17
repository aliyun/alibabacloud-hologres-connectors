package com.alibaba.hologres.performace.client;

import com.alibaba.hologres.client.model.HoloVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;

public class Reporter {
  public static Logger LOG = LoggerFactory.getLogger(Reporter.class);
  private String confName;
  private File dir;
  private String version;

  public Reporter(String confName) {
    this.confName = confName;
    this.dir = new File(confName).getParentFile();
  }

  long start = 0L;

  public void start(HoloVersion version) {
    this.start = System.currentTimeMillis();
    this.version = String.format("%s.%s.%s", version.getMajorVersion(), version.getMinorVersion(),
        version.getFixVersion());
  }

  public void report(long count, double qps1, double qps5, double qps15, double latencyMean, double latencyP99,
      double latencyP999) {
    File file = new File(dir, "result.csv");
    try (BufferedWriter bw = new BufferedWriter(
        new OutputStreamWriter(new FileOutputStream(file)))) {
      bw.write("start,end,count,qps1,qps5,qps15,latencyMean,latencyP99,latencyP999,version\n");
      bw.write(String.valueOf(start));
      bw.write(',');
      bw.write(String.valueOf(System.currentTimeMillis()));
      bw.write(',');
      bw.write(String.valueOf(count));
      bw.write(',');
      bw.write(String.valueOf(qps1));
      bw.write(',');
      bw.write(String.valueOf(qps5));
      bw.write(',');
      bw.write(String.valueOf(qps15));
      bw.write(',');
      bw.write(String.valueOf(latencyMean));
      bw.write(',');
      bw.write(String.valueOf(latencyP99));
      bw.write(',');
      bw.write(String.valueOf(latencyP999));
      bw.write(',');
      bw.write(version);
    } catch (Exception e) {
      LOG.error("", e);
    }
  }
}
