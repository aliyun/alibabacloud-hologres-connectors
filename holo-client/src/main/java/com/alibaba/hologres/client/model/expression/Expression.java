package com.alibaba.hologres.client.model.expression;

import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.model.HoloVersion;

import java.io.Serializable;
import java.util.Objects;

public class Expression implements Serializable {
    public final String conflictUpdateSet;
    public final String conflictWhere;

    public static final HoloVersion INSERT_SUPPORT_VERSION = new HoloVersion(3, 2, 0);
    public static final HoloVersion DELETE_SUPPORT_VERSION = new HoloVersion(4, 0, 0);

    public Expression(String conflictUpdateSet, String conflictWhere) {
        this.conflictUpdateSet = conflictUpdateSet;
        this.conflictWhere = conflictWhere;
    }

    public String toString() {
        return "conflictUpdateSet:" + conflictUpdateSet + '\n' + "conflictWhere:" + conflictWhere;
    }

    public boolean hasConflictUpdateSet() {
        return conflictUpdateSet != null && !conflictUpdateSet.isEmpty();
    }

    public boolean hasConflictWhere() {
        return conflictWhere != null && !conflictWhere.isEmpty();
    }

    public boolean hasExpr() {
        return hasConflictUpdateSet() || hasConflictWhere();
    }

    public static boolean isVersionSupport(HoloVersion version, Put.MutationType mutationType) {
        if (mutationType == Put.MutationType.INSERT
                && version.compareTo(INSERT_SUPPORT_VERSION) < 0) {
            return false;
        }
        if (mutationType == Put.MutationType.DELETE
                && version.compareTo(DELETE_SUPPORT_VERSION) < 0) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(conflictUpdateSet, conflictWhere);
    }
}
