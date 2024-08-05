package com.alibaba.hologres.kafka.utils;

import com.alibaba.hologres.kafka.model.DirtyDataStrategy;

import java.util.List;

/** DirtyDataUtils. */
public class DirtyDataUtils {
    DirtyDataStrategy dirtyDataStrategy;
    List<String> dirtyDataToSkipOnce = null;

    public DirtyDataUtils(DirtyDataStrategy dirtyDataStrategy) {
        this.dirtyDataStrategy = dirtyDataStrategy;
    }

    public DirtyDataUtils(DirtyDataStrategy dirtyDataStrategy, List<String> dirtyDataToSkipOnce) {
        this.dirtyDataStrategy = dirtyDataStrategy;
        this.dirtyDataToSkipOnce = dirtyDataToSkipOnce;
    }

    public void setDirtyDataStrategy(DirtyDataStrategy dirtyDataStrategy) {
        this.dirtyDataStrategy = dirtyDataStrategy;
    }

    public DirtyDataStrategy getDirtyDataStrategy() {
        return dirtyDataStrategy;
    }

    public void setDirtyDataToSkipOnce(List<String> dirtyDataToSkipOnce) {
        this.dirtyDataToSkipOnce = dirtyDataToSkipOnce;
    }

    public List<String> getDirtyDataToSkipOnce() {
        return dirtyDataToSkipOnce;
    }
}
