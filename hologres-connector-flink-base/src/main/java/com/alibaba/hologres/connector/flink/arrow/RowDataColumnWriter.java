package com.alibaba.hologres.connector.flink.arrow;

import org.apache.flink.table.data.RowData;

/**
 * 从 Flink RowData 直接写入 Arrow 向量的列写入器接口. 每个实现对应一种数据类型，直接调用 Arrow 向量的原生 set 方法， 避免 Object
 * 装箱/拆箱和中间类型转换.
 */
public interface RowDataColumnWriter {

    /**
     * 将 RowData 中指定位置的值写入 Arrow 向量的指定行. 调用方需保证该位置不为 null（null 由 setNull 处理）.
     *
     * @param rowId Arrow 向量中的行号
     * @param rowData Flink RowData
     * @param pos RowData 中的列位置
     */
    void write(int rowId, RowData rowData, int pos);

    /**
     * 将 Arrow 向量的指定行设为 null.
     *
     * @param rowId Arrow 向量中的行号
     */
    void setNull(int rowId);
}
