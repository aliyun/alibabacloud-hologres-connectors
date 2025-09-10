package com.alibaba.hologres.client.test;

import com.alibaba.hologres.client.ddl.DDLGenerator;
import com.alibaba.hologres.client.model.Column;
import com.alibaba.hologres.client.model.TableName;
import com.alibaba.hologres.client.model.TableSchema;

import java.util.ArrayList;
import java.util.List;

/** Created by liangmei.gl. Date: 2020-12-22 21:45. */
public class GeneratorDemo {

    public static void main(String[] args) throws Exception {

        TableSchema.Builder builder = new TableSchema.Builder();
        builder.setTableName(TableName.valueOf("generator3"));
        builder.setComment("lmtest");

        //        builder.setSensitive(true);

        List<Column> columnList = new ArrayList<>();
        Column c1 = new Column();
        c1.setName("A1");
        c1.setTypeName("text");
        //        c1.setAllowNull(true);
        c1.setPrimaryKey(true);
        columnList.add(c1);
        builder.setColumns(columnList);
        builder.setPartitionColumnName("a1");

        builder.setClusteringKey(new String[] {"A1"});
        builder.setBitmapIndexKey(new String[] {"A1"});
        TableSchema table = builder.build();
        System.out.println(DDLGenerator.sqlGenerator(table));
    }
}
