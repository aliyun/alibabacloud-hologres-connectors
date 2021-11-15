/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.hologres.hive;

import com.alibaba.hologres.hive.conf.HoloStorageConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/** HoloSerDe. */
public class HoloSerDe extends AbstractSerDe {

    private static final Logger LOGGER = LoggerFactory.getLogger(HoloSerDe.class);

    HoloRecordWritable dbRecordWritable;

    private StructObjectInspector objectInspector;
    private int numColumns;
    private String[] hiveColumnTypeArray;
    private List<String> columnNames;
    private List<JDBCType> columnTypes;
    private List<Object> row;

    private PrimitiveTypeInfo[] hiveColumnTypes;

    /*
     * This method gets called multiple times by Hive. On some invocations, the properties will be empty.
     * We need to detect when the properties are not empty to initialise the class variables.
     *
     * @see org.apache.hadoop.hive.serde2.Deserializer#initialize(org.apache.hadoop.conf.Configuration, java.util.Properties)
     */
    @Override
    public void initialize(Configuration conf, Properties props) throws SerDeException {
        try {
            LOGGER.debug("Initializing the SerDe");

            String url = props.getProperty(HoloStorageConfig.JDBC_URL.getPropertyName());
            String tableName = props.getProperty(HoloStorageConfig.TABLE.getPropertyName());
            String username = props.getProperty(HoloStorageConfig.USERNAME.getPropertyName());
            String password = props.getProperty(HoloStorageConfig.PASSWORD.getPropertyName());
            LOGGER.info("tbl properties:{}", props);
            LOGGER.info("url:{}", url);
            LOGGER.info("tableName:{}", tableName);

            if (tableName == null || tableName.isEmpty()) {
                throw new Exception(
                        HoloStorageConfig.TABLE.getPropertyName() + " should be defined");
            }
            if (username == null || username.isEmpty()) {
                throw new Exception(
                        HoloStorageConfig.USERNAME.getPropertyName() + " should be defined");
            }
            if (password == null || password.isEmpty()) {
                throw new Exception(
                        HoloStorageConfig.PASSWORD.getPropertyName() + " should be defined");
            }
            DatabaseMetaAccessor dbAccessor =
                    new DatabaseMetaAccessor(url, tableName, username, password);
            List<Column> columns = dbAccessor.getColumns();
            columnNames = new ArrayList<>(columns.size());
            columnTypes = new ArrayList<>(columns.size());
            for (Column column : columns) {
                columnNames.add(column.getName());
                columnTypes.add(JDBCType.valueOf(column.getType()));
            }
            numColumns = columnNames.size();

            String[] hiveColumnNameArray =
                    parseProperty(props.getProperty(serdeConstants.LIST_COLUMNS), ",");
            if (numColumns != hiveColumnNameArray.length) {
                throw new SerDeException(
                        "Expected "
                                + numColumns
                                + " columns. Table definition has "
                                + hiveColumnNameArray.length
                                + " columns");
            }
            List<String> hiveColumnNames = Arrays.asList(hiveColumnNameArray);

            hiveColumnTypeArray =
                    parseProperty(props.getProperty(serdeConstants.LIST_COLUMN_TYPES), ":");
            if (hiveColumnTypeArray.length == 0) {
                throw new SerDeException("Received an empty Hive column type definition");
            }

            List<TypeInfo> hiveColumnTypesList =
                    TypeInfoUtils.getTypeInfosFromTypeString(
                            props.getProperty(serdeConstants.LIST_COLUMN_TYPES));

            hiveColumnTypes = new PrimitiveTypeInfo[hiveColumnTypesList.size()];
            List<ObjectInspector> fieldInspectors = new ArrayList<>(hiveColumnNames.size());
            for (int i = 0; i < hiveColumnNames.size(); i++) {
                TypeInfo ti = hiveColumnTypesList.get(i);
                if (ti.getCategory() != ObjectInspector.Category.PRIMITIVE) {
                    throw new SerDeException("Non primitive types not supported yet");
                }
                hiveColumnTypes[i] = (PrimitiveTypeInfo) ti;
                fieldInspectors.add(
                        PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                                hiveColumnTypes[i]));
            }

            objectInspector =
                    ObjectInspectorFactory.getStandardStructObjectInspector(
                            hiveColumnNames, fieldInspectors);
            row = new ArrayList<>(numColumns);

            dbRecordWritable = new HoloRecordWritable(columnNames.size());
        } catch (Exception e) {
            LOGGER.error("Caught exception while initializing the SqlSerDe", e);
            throw new SerDeException(e);
        }
    }

    private String[] parseProperty(String propertyValue, String delimiter) {
        if ((propertyValue == null) || (propertyValue.trim().isEmpty())) {
            return new String[] {};
        }

        return propertyValue.split(delimiter);
    }

    @Override
    public Object deserialize(Writable blob) throws SerDeException {
        LOGGER.debug("Deserializing from SerDe");
        if (!(blob instanceof MapWritable)) {
            throw new SerDeException("Expected MapWritable. Got " + blob.getClass().getName());
        }

        if ((row == null) || (columnNames == null)) {
            throw new SerDeException("Holo SerDe has no columns to deserialize");
        }

        row.clear();
        MapWritable input = (MapWritable) blob;
        Text columnKey = new Text();
        for (int i = 0; i < numColumns; i++) {
            columnKey.set(columnNames.get(i));
            Writable value = input.get(columnKey);

            if (value == NullWritable.get()) {
                row.add(null);
            } else {
                switch (columnTypes.get(i)) {
                    case TINYINT:
                        row.add(Byte.valueOf(value.toString()));
                        break;
                    case SMALLINT:
                        row.add(Short.valueOf(value.toString()));
                        break;
                    case INTEGER:
                        row.add(Integer.valueOf(value.toString()));
                        break;
                    case BIGINT:
                        row.add(Long.valueOf(value.toString()));
                        break;
                    case REAL:
                    case FLOAT:
                        row.add(Float.valueOf(value.toString()));
                        break;
                    case DOUBLE:
                        row.add(Double.valueOf(value.toString()));
                        break;
                    case NUMERIC:
                    case DECIMAL:
                        row.add(new HiveDecimalWritable(value.toString()).getHiveDecimal());
                        break;
                    case BIT:
                    case BOOLEAN:
                        row.add(Boolean.valueOf(value.toString()));
                        break;
                    case CHAR:
                    case VARCHAR:
                    case LONGVARCHAR:
                        row.add(String.valueOf(value.toString()));
                        break;
                    case DATE:
                        row.add(java.sql.Date.valueOf(value.toString()));
                        break;
                    case TIMESTAMP:
                        row.add(java.sql.Timestamp.valueOf(value.toString()));
                        break;
                    case BINARY:
                    case VARBINARY:
                        row.add(value.toString().getBytes(StandardCharsets.UTF_8));
                        break;
                    case ARRAY:
                    default:
                        // do nothing
                        break;
                }
            }
        }
        return row;
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return objectInspector;
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return MapWritable.class;
    }

    @Override
    public HoloRecordWritable serialize(Object row, ObjectInspector objInspector)
            throws SerDeException {
        LOGGER.trace("Serializing from SerDe");
        if ((row == null) || (hiveColumnTypes == null)) {
            throw new SerDeException("Holo SerDe has no columns to serialize");
        }

        if (((Object[]) row).length != numColumns) {
            throw new SerDeException(
                    String.format(
                            "Required %d columns, received %d.",
                            numColumns, ((Object[]) row).length));
        }

        dbRecordWritable.clear();
        for (int i = 0; i < numColumns; i++) {
            Object rowData = ((Object[]) row)[i];
            if (null != rowData) {
                switch (hiveColumnTypes[i].getPrimitiveCategory()) {
                    case INT:
                        rowData = Integer.valueOf(rowData.toString());
                        break;
                    case SHORT:
                        rowData = Short.valueOf(rowData.toString());
                        break;
                    case BYTE:
                        rowData = Byte.valueOf(rowData.toString());
                        break;
                    case LONG:
                        rowData = Long.valueOf(rowData.toString());
                        break;
                    case FLOAT:
                        rowData = Float.valueOf(rowData.toString());
                        break;
                    case DOUBLE:
                        rowData = Double.valueOf(rowData.toString());
                        break;
                    case DECIMAL:
                        rowData =
                                new HiveDecimalWritable(rowData.toString())
                                        .getHiveDecimal()
                                        .bigDecimalValue();
                        break;
                    case BOOLEAN:
                        rowData = Boolean.valueOf(rowData.toString());
                        break;
                    case CHAR:
                    case VARCHAR:
                    case STRING:
                        rowData = String.valueOf(rowData.toString());
                        break;
                    case DATE:
                        rowData = java.sql.Date.valueOf(rowData.toString());
                        break;
                    case TIMESTAMP:
                        rowData = java.sql.Timestamp.valueOf(rowData.toString());
                        break;
                    case BINARY:
                        rowData = rowData.toString().getBytes(StandardCharsets.UTF_8);
                        break;
                    default:
                        // do nothing
                        break;
                }
            }
            dbRecordWritable.set(i, rowData);
        }
        return dbRecordWritable;
    }

    @Override
    public SerDeStats getSerDeStats() {
        return null;
    }
}
