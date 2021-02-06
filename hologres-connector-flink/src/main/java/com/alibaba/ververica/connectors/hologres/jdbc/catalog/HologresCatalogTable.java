package com.alibaba.ververica.connectors.hologres.jdbc.catalog;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;

import java.util.HashMap;
import java.util.Map;

/** Catalog table for Hologres. */
public class HologresCatalogTable extends CatalogTableImpl {
	public HologresCatalogTable(TableSchema tableSchema, Map<String, String> properties, String comment) {
		super(tableSchema, properties, comment);
	}

	@Override
	public CatalogTable copy(Map<String, String> var1) {
		return new CatalogTableImpl(getSchema(), getPartitionKeys(), new HashMap<>(var1), getComment());
	}
}
