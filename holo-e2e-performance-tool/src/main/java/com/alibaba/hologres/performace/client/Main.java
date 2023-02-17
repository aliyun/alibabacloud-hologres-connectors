package com.alibaba.hologres.performace.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
  public static final Logger LOG = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {
    if (args.length != 2) {
      LOG.error("invalid args\njava -jar xxxx.jar CONF_NAME METHOD\n METHOD = INSERT/FIXED_COPY/GET/SCAN");
      return;
    }
    try {
      switch (args[1]) {
        case "INSERT":
          new InsertTest().run(args[0]);
          break;
        case "GET":
          new GetTest().run(args[0]);
          break;
        case "PREPARE_GET_DATA":
          new PrepareGetData().run(args[0]);
          break;
        case "FIXED_COPY":
          new FixedCopyTest().run(args[0]);
          break;
        case "PREPARE_SCAN_DATA":
          new PrepareScanData().run(args[0]);
          break;
        case "SCAN":
          new ScanTest().run(args[0]);
          break;
        default:
          throw new Exception("unknow method " + args[1]);
      }
    } catch (Exception e) {
      LOG.error("", e);
    }
  }
}
