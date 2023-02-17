package com.alibaba.hologres.performace.client;

import com.alibaba.hologres.client.utils.ConfLoader;


public class PrepareScanData extends FixedCopyTest{
    ScanTestConf scanTestConf = new ScanTestConf();
    PrepareScanDataConf prepareScanDataConf = new PrepareScanDataConf();

    @Override
    void init() throws Exception {
        super.init();
        ConfLoader.load(confName, "scan.", scanTestConf);
        ConfLoader.load(confName, "prepareScanData.", prepareScanDataConf);
        conf.prefixPk = true;
        conf.recordCountPerPrefix = prepareScanDataConf.recordCountPerPrefix;
        conf.testByTime = false;
        conf.rowNumber = prepareScanDataConf.rowNumber;
        conf.tableName = scanTestConf.tableName;
        conf.hasPk = true;
        conf.orientation = prepareScanDataConf.orientation;
        conf.deleteTableAfterDone = false;
    }
}

class PrepareScanDataConf {
    public int recordCountPerPrefix = 100;
    public long rowNumber = 1000000;
    public String orientation = "row";
}
