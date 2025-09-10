package com.alibaba.hologres.client.model.expression;

import com.alibaba.hologres.client.model.Record;
import com.alibaba.hologres.client.model.TableSchema;

import java.io.Serializable;

public class RecordWithExpression extends Record implements Serializable {
    private Expression expression;

    public RecordWithExpression(
            TableSchema schema, String conflictUpdateSet, String conflictWhere) {
        super(schema);
        this.expression = new Expression(conflictUpdateSet, conflictWhere);
    }

    public RecordWithExpression(Record record, String conflictUpdateSet, String conflictWhere) {
        super(record.getSchema());
        this.merge(record);
        this.setType(record.getType());
        this.expression = new Expression(conflictUpdateSet, conflictWhere);
    }

    public Expression getExpression() {
        return expression;
    }

    @Override
    public String toString() {
        return "RecordWithExpression{" + expression.toString() + '\n' + super.toString() + '}';
    }

    public static class Builder {
        TableSchema schema;
        Record record;
        String conflictUpdateSet;
        String conflictWhere;

        public Builder(TableSchema schema) {
            this.schema = schema;
        }

        public Builder(Record record) {
            this.record = record;
        }

        public Builder setConflictUpdateSet(String conflictUpdateSet) {
            this.conflictUpdateSet = conflictUpdateSet;
            return this;
        }

        public Builder setConflictWhere(String conflictWhere) {
            this.conflictWhere = conflictWhere;
            return this;
        }

        public RecordWithExpression build() {
            if (record == null) {
                return new RecordWithExpression(schema, conflictUpdateSet, conflictWhere);
            }
            return new RecordWithExpression(record, conflictUpdateSet, conflictWhere);
        }
    }
}
