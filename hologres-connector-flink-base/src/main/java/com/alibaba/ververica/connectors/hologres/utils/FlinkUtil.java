package com.alibaba.ververica.connectors.hologres.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.types.logical.RowType;

import com.alibaba.ververica.connectors.common.util.FactoryOptionUtil;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

/** Flink compatibility util. */
public class FlinkUtil {
    @SuppressWarnings("unchecked")
    public static TypeInformation<RowData> getRowTypeInfo(TableSchema tableSchema) {
        try {
            Class rowDataTypeInfoClass = getTypeInfoClass();
            Method method = rowDataTypeInfoClass.getMethod("of", RowType.class);
            Object result =
                    method.invoke(null, (RowType) tableSchema.toRowDataType().getLogicalType());
            return (TypeInformation<RowData>) result;
        } catch (ClassNotFoundException
                | NoSuchMethodException
                | IllegalAccessException
                | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private static Class getTypeInfoClass() throws ClassNotFoundException {
        try {
            // Flink 1.11
            return Class.forName("org.apache.flink.table.runtime.typeutils.RowDataTypeInfo");
        } catch (ClassNotFoundException e) {
            // Flink 1.12+
            return Class.forName("org.apache.flink.table.runtime.typeutils.InternalTypeInfo");
        }
    }

    public static void transformContext(
            DynamicTableFactory factory, DynamicTableFactory.Context context) {
        Map<String, String> catalogOptions = context.getCatalogTable().getOptions();
        Map<String, String> convertedOptions =
                FactoryOptionUtil.normalizeOptionCaseAsFactory(factory, catalogOptions);
        catalogOptions.clear();

        for (Map.Entry<String, String> entry : convertedOptions.entrySet()) {
            catalogOptions.put(entry.getKey(), entry.getValue());
        }
    }
}
