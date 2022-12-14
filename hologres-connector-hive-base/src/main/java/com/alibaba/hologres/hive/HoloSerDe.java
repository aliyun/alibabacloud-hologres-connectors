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

import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.hive.conf.HoloClientParam;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
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
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/** HoloSerDe. */
public class HoloSerDe extends AbstractSerDe {

    private static final Logger LOGGER = LoggerFactory.getLogger(HoloSerDe.class);

    HoloRecordWritable dbRecordWritable;

    private StructObjectInspector objectInspector;
    private int hiveColumnCount;
    private String[] hiveColumnNames;
    private Column[] holoColumns;
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
        HoloClientParam param = new HoloClientParam(conf, props);
        HoloClientProvider clientProvider = new HoloClientProvider(param);
        try {
            TableSchema schema = clientProvider.getTableSchema();
            holoColumns = schema.getColumnSchema();
            hiveColumnNames = parseProperty(props.getProperty(serdeConstants.LIST_COLUMNS), ",");
            hiveColumnCount = hiveColumnNames.length;
            if (holoColumns.length < hiveColumnCount) {
                throw new SerDeException(
                        String.format(
                                "Table definition has %s columns, could not greater than Hologres table %s has %d columns.",
                                hiveColumnCount, schema.getTableName(), holoColumns.length));
            }

            String[] hiveColumnTypeArray =
                    parseProperty(props.getProperty(serdeConstants.LIST_COLUMN_TYPES), ":");
            if (hiveColumnTypeArray.length == 0) {
                throw new SerDeException("Received an empty Hive column type definition");
            }

            List<TypeInfo> hiveColumnTypesList =
                    TypeInfoUtils.getTypeInfosFromTypeString(
                            props.getProperty(serdeConstants.LIST_COLUMN_TYPES));

            hiveColumnTypes = new PrimitiveTypeInfo[hiveColumnTypesList.size()];
            List<ObjectInspector> fieldInspectors = new ArrayList<>(hiveColumnCount);
            for (int i = 0; i < hiveColumnCount; i++) {
                TypeInfo ti = hiveColumnTypesList.get(i);
                if (ti.getCategory() != ObjectInspector.Category.PRIMITIVE) {
                    throw new SerDeException("Non primitive types not supported yet");
                }
                hiveColumnTypes[i] = (PrimitiveTypeInfo) ti;
                fieldInspectors.add(
                        PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                                hiveColumnTypes[i]));
            }
            validateColumns(schema, hiveColumnNames, hiveColumnTypes);

            objectInspector =
                    ObjectInspectorFactory.getStandardStructObjectInspector(
                            Arrays.asList(hiveColumnNames), fieldInspectors);
            row = new ArrayList<>(hiveColumnCount);

            dbRecordWritable = new HoloRecordWritable(hiveColumnCount, hiveColumnNames);
        } catch (Exception e) {
            LOGGER.error("Caught exception while initializing the SqlSerDe", e);
            throw new SerDeException(e);
        } finally {
            clientProvider.closeClient();
        }
    }

    /** 创建表时验证数据类型. */
    private void validateColumns(
            TableSchema schema, String[] hiveColumnNames, PrimitiveTypeInfo[] hiveColumnTypes) {
        String columnName;
        PrimitiveCategory columnType;

        for (int i = 0; i < hiveColumnCount; i++) {
            columnName = hiveColumnNames[i];
            columnType = hiveColumnTypes[i].getPrimitiveCategory();
            Column holoColumn;
            try {
                holoColumn = schema.getColumn(schema.getColumnIndex(columnName));
            } catch (NullPointerException e) {
                throw new IllegalArgumentException(
                        String.format(
                                "Column %s with data type %s does not exist in hologres!",
                                columnName, columnType));
            }
            boolean matched = false;
            switch (holoColumn.getType()) {
                case Types.TINYINT:
                    matched = (columnType == PrimitiveCategory.BYTE);
                    break;
                case Types.SMALLINT:
                    matched = (columnType == PrimitiveCategory.SHORT);
                    break;
                case Types.INTEGER:
                    matched = (columnType == PrimitiveCategory.INT);
                    break;
                case Types.BIGINT:
                    matched = (columnType == PrimitiveCategory.LONG);
                    break;
                case Types.REAL:
                case Types.FLOAT:
                    matched = (columnType == PrimitiveCategory.FLOAT);
                    break;
                case Types.DOUBLE:
                    matched = (columnType == PrimitiveCategory.DOUBLE);
                    break;
                case Types.NUMERIC:
                case Types.DECIMAL:
                    matched = (columnType == PrimitiveCategory.DECIMAL);
                    break;
                case Types.BOOLEAN:
                case Types.BIT:
                    matched = (columnType == PrimitiveCategory.BOOLEAN);
                    break;
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                    matched = (columnType == PrimitiveCategory.STRING);
                    break;
                case Types.DATE:
                    matched = (columnType == PrimitiveCategory.DATE);
                    break;
                case Types.TIMESTAMP:
                    matched = (columnType == PrimitiveCategory.TIMESTAMP);
                    break;
                case Types.BINARY:
                case Types.VARBINARY:
                    matched = (columnType == PrimitiveCategory.BINARY);
                    break;
                case Types.OTHER:
                    switch (holoColumn.getTypeName()) {
                        case "json":
                        case "jsonb":
                            matched = (columnType == PrimitiveCategory.STRING);
                            break;
                        default:
                            throw new IllegalArgumentException(
                                    String.format(
                                            "Does not support column %s with data type %s and hologres type %s for now!",
                                            columnName, columnType, holoColumn.getTypeName()));
                    }
                    break;
                default:
                    throw new IllegalArgumentException(
                            String.format(
                                    "Does not support column %s with data type %s and hologres type %s for now!",
                                    columnName, columnType, holoColumn.getTypeName()));
            }
            if (!matched) {
                throw new IllegalArgumentException(
                        String.format(
                                "Column %s with data type %s does not match the hologres data type!",
                                columnName, columnType));
            }
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

        if (row == null || hiveColumnTypes == null) {
            throw new SerDeException("Holo SerDe has no columns to deserialize");
        }

        row.clear();
        MapWritable input = (MapWritable) blob;
        Text columnKey = new Text();
        for (int i = 0; i < hiveColumnCount; i++) {
            columnKey.set(hiveColumnNames[i]);
            Writable value = input.get(columnKey);

            if (value == NullWritable.get()) {
                row.add(null);
            } else {
                switch (hiveColumnTypes[i].getPrimitiveCategory()) {
                    case BYTE:
                        row.add(Byte.valueOf(value.toString()));
                        break;
                    case SHORT:
                        row.add(Short.valueOf(value.toString()));
                        break;
                    case INT:
                        row.add(Integer.valueOf(value.toString()));
                        break;
                    case LONG:
                        row.add(Long.valueOf(value.toString()));
                        break;
                    case FLOAT:
                        row.add(Float.valueOf(value.toString()));
                        break;
                    case DOUBLE:
                        row.add(Double.valueOf(value.toString()));
                        break;
                    case DECIMAL:
                        row.add(new HiveDecimalWritable(value.toString()).getHiveDecimal());
                        break;
                    case BOOLEAN:
                        row.add(Boolean.valueOf(value.toString()));
                        break;
                    case CHAR:
                    case VARCHAR:
                    case STRING:
                        row.add(String.valueOf(value.toString()));
                        break;
                    case DATE:
                        row.add(java.sql.Date.valueOf(value.toString()));
                        break;
                    case TIMESTAMP:
                        row.add(java.sql.Timestamp.valueOf(value.toString()));
                        break;
                    case BINARY:
                        row.add(value.toString().getBytes(StandardCharsets.UTF_8));
                        break;
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
        if (row == null || hiveColumnTypes == null) {
            throw new SerDeException("Holo SerDe has no columns to serialize");
        }

        if (((Object[]) row).length != hiveColumnCount) {
            throw new SerDeException(
                    String.format(
                            "Required %d columns, received %d.",
                            hiveColumnCount, ((Object[]) row).length));
        }

        dbRecordWritable.clear();
        for (int i = 0; i < hiveColumnCount; i++) {
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
