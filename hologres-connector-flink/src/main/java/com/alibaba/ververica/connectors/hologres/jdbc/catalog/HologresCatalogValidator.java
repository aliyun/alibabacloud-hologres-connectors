package com.alibaba.ververica.connectors.hologres.jdbc.catalog;

import org.apache.flink.table.descriptors.CatalogDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;

/** Validator for HologresCatalog. */
public class HologresCatalogValidator extends CatalogDescriptorValidator {
	public static final String CATALOG_TYPE_VALUE_JDBC = "hologres";

	public static final String CATALOG_HOLOGRES_USERNAME = "username";
	public static final String CATALOG_HOLOGRES_PASSWORD = "password";
	public static final String CATALOG_HOLOGRES_ENDPOINT = "endpoint";

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		properties.validateValue(CATALOG_TYPE, CATALOG_TYPE_VALUE_JDBC, false);
		properties.validateString(CATALOG_HOLOGRES_ENDPOINT, false, 1);
		properties.validateString(CATALOG_HOLOGRES_USERNAME, false, 1);
		properties.validateString(CATALOG_HOLOGRES_PASSWORD, false, 1);
		properties.validateString(CATALOG_DEFAULT_DATABASE, false, 1);
	}
}
