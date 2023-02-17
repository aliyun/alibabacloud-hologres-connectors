package com.alibaba.hologres.performace.client;

import com.alibaba.hologres.client.utils.ConfLoader;


public class PrepareGetData extends FixedCopyTest{
    GetTestConf getTestConf = new GetTestConf();
    PrepareGetDataConf prepareGetDataConf = new PrepareGetDataConf();

    @Override
    void init() throws Exception {
        super.init();
        ConfLoader.load(confName, "get.", getTestConf);
        ConfLoader.load(confName, "prepareGetData.", prepareGetDataConf);
        conf.testByTime = false;
        conf.rowNumber = prepareGetDataConf.rowNumber;
        conf.tableName = getTestConf.tableName;
        conf.hasPk = true;
        conf.orientation = prepareGetDataConf.orientation;
        conf.deleteTableAfterDone = false;
    }

}

class PrepareGetDataConf {
    public long rowNumber = 1000000;
    public String orientation = "row";
}
