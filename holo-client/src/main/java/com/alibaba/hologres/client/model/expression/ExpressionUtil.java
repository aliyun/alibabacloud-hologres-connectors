package com.alibaba.hologres.client.model.expression;

import com.alibaba.hologres.client.exception.ExceptionCode;
import com.alibaba.hologres.client.exception.HoloClientWithDetailsException;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.update.UpdateSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ExpressionUtil {

    public static final Logger LOGGER = LoggerFactory.getLogger(ExpressionUtil.class);

    public static void CheckExpr(RecordWithExpression record)
            throws HoloClientWithDetailsException {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("UPDATE tbl AS old SET ");
        stringBuilder.append(
                record.getExpression().hasConflictUpdateSet()
                        ? record.getExpression().conflictUpdateSet
                        : "col = 1 ");
        if (record.getExpression().hasConflictWhere()) {
            stringBuilder.append(" WHERE ").append(record.getExpression().conflictWhere);
        }
        String query = stringBuilder.toString();
        net.sf.jsqlparser.statement.Statement statement;
        try {
            statement = CCJSqlParserUtil.parse(query);
        } catch (JSQLParserException e) {
            LOGGER.error("Parse expression failed", e);
            throw new HoloClientWithDetailsException(
                    ExceptionCode.INVALID_REQUEST, e.getMessage(), record);
        }

        if (!(statement instanceof net.sf.jsqlparser.statement.update.Update)) {
            throw new HoloClientWithDetailsException(
                    ExceptionCode.INVALID_REQUEST,
                    String.format(
                            "Parse expression failed, conflict update:%s,  conflcit where:%s",
                            record.getExpression().conflictUpdateSet,
                            record.getExpression().conflictWhere),
                    record);
        }
        net.sf.jsqlparser.statement.update.Update updateStatement =
                (net.sf.jsqlparser.statement.update.Update) statement;
        if (updateStatement.getOrderByElements() != null
                || updateStatement.getLimit() != null
                || updateStatement.getReturningExpressionList() != null
                || updateStatement.getJoins() != null
                || updateStatement.getOutputClause() != null
                || updateStatement.getModifierPriority() != null) {
            throw new HoloClientWithDetailsException(
                    ExceptionCode.INVALID_REQUEST,
                    String.format(
                            "Unsupported expression syntax, conflict update:%s,  conflcit where:%s",
                            record.getExpression().conflictUpdateSet,
                            record.getExpression().conflictWhere),
                    record);
        }
        if (record.getExpression().hasConflictUpdateSet()) {
            for (UpdateSet updateSet : updateStatement.getUpdateSets()) {
                for (net.sf.jsqlparser.schema.Column column : updateSet.getColumns()) {
                    String colName = column.getColumnName();
                    colName =
                            (colName.indexOf("\"") == 0
                                            && colName.lastIndexOf("\"") == colName.length() - 1)
                                    ? colName.substring(1, colName.length() - 1)
                                    : colName;
                    Integer columnIndex = record.getSchema().getColumnIndex(colName);
                    if (columnIndex == null) {
                        throw new HoloClientWithDetailsException(
                                ExceptionCode.INVALID_REQUEST,
                                String.format(
                                        "Column %s not found in schema", column.getColumnName()),
                                record);
                    }
                    com.alibaba.hologres.client.model.Column colMeta =
                            record.getSchema().getColumn(columnIndex);
                    if (colMeta.getDefaultValue() == null && !record.getBitSet().get(columnIndex)) {
                        throw new HoloClientWithDetailsException(
                                ExceptionCode.INVALID_REQUEST,
                                String.format(
                                        "Column %s in the conflict update must be set to a non-conflicting value in the record.",
                                        column.getColumnName()),
                                record);
                    }
                    for (Expression expression : updateSet.getExpressions()) {
                        List<net.sf.jsqlparser.schema.Column> columns = findColumns(expression);
                        for (net.sf.jsqlparser.schema.Column col : columns) {
                            colName = col.getColumnName();
                            colName =
                                    (colName.indexOf("\"") == 0
                                                    && colName.lastIndexOf("\"")
                                                            == colName.length() - 1)
                                            ? colName.substring(1, colName.length() - 1)
                                            : colName;
                            if (record.getSchema().getColumnIndex(colName) == null) {
                                throw new HoloClientWithDetailsException(
                                        ExceptionCode.INVALID_REQUEST,
                                        String.format(
                                                "Column %s not found in schema",
                                                col.getColumnName()),
                                        record);
                            }
                        }
                    }
                }
            }
        }
        if (record.getExpression().hasConflictWhere()) {
            Expression whereExpr = updateStatement.getWhere();
            List<net.sf.jsqlparser.schema.Column> columns = findColumns(whereExpr);
            for (net.sf.jsqlparser.schema.Column col : columns) {
                String colName = col.getColumnName();
                colName =
                        (colName.indexOf("\"") == 0
                                        && colName.lastIndexOf("\"") == colName.length() - 1)
                                ? colName.substring(1, colName.length() - 1)
                                : colName;
                if (record.getSchema().getColumnIndex(colName) == null) {
                    throw new HoloClientWithDetailsException(
                            ExceptionCode.INVALID_REQUEST,
                            String.format("Column %s not found in schema", col.getColumnName()),
                            record);
                }
            }
        }
        return;
    }

    // Method to find columns in an expression
    public static List<net.sf.jsqlparser.schema.Column> findColumns(Expression expression) {
        List<Column> columns = new ArrayList<>();
        expression.accept(
                new ExpressionVisitorAdapter() {
                    @Override
                    public void visit(Column column) {
                        columns.add(column);
                    }
                });
        return columns;
    }
}
