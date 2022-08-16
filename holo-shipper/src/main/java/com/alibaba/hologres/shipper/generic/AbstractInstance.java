package com.alibaba.hologres.shipper.generic;

import java.util.List;
import java.util.Map;

public abstract class AbstractInstance {
    public abstract List<String> getDBList();
    public abstract AbstractDB getDB(String dbName);
    public abstract List<String> getAllRoles();
    public abstract void createRoles(List<String> roles, Map<String, Boolean> roleInfo);
    public abstract void showVersion();
    public abstract void createDB(String dbName);
    public abstract Map<String, Boolean> getRoleInfo(List<String> roleList); //true: role can login, false: role cannot login
    public void close() {}
}
