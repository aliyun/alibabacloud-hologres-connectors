package com.alibaba.hologres;

public class IncompatibleResult {

    public IncompatibleResult(String deploymentName, String deploymentVersion, String tableName) {
        this.deploymentName = deploymentName;
        this.deploymentVersion = deploymentVersion;
        this.tableName = tableName;
    }
    public String deploymentName;
    public String deploymentVersion;
    public String tableName;
}
