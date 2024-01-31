package com.alibaba.hologres.performace.client;

import com.alibaba.hologres.client.utils.ConfLoader;


public class PrepareBinlogData extends FixedCopyTest{
    BinlogTestConf binlogTestConf = new BinlogTestConf();
    PrepareBinlogDataConf prepareBinlogDataConf = new PrepareBinlogDataConf();

    @Override
    void init() throws Exception {
        super.init();
        ConfLoader.load(confName, "binlog.", binlogTestConf);
        ConfLoader.load(confName, "prepareBinlogData.", prepareBinlogDataConf);
        conf.testByTime = false;
        conf.rowNumber = prepareBinlogDataConf.rowNumber;
        conf.tableName = binlogTestConf.tableName;
        conf.publicationName = binlogTestConf.publicationName;
        conf.slotName = binlogTestConf.slotName;
        conf.recreatePublicationAndSlot = prepareBinlogDataConf.recreatePublicationAndSlot;
        conf.binlogTTL = prepareBinlogDataConf.binlogTTL;
        conf.shardCount = prepareBinlogDataConf.shardCount;
        conf.hasPk = true;
        conf.enableBinlogConsumption = true;
        conf.enableBinlog = true;
        conf.deleteTableAfterDone = false;
        conf.dumpMemoryStat = false;
    }

}

class PrepareBinlogDataConf {
    public long rowNumber = 1000000;
    public int shardCount = 40;
    public boolean recreatePublicationAndSlot = true;
    public int binlogTTL = 19600; //6 hours
}
