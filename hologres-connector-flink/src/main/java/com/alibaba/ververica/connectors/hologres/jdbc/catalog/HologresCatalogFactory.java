package com.alibaba.ververica.connectors.hologres.jdbc.catalog;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.CatalogFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.ververica.connectors.hologres.jdbc.catalog.HologresCatalogValidator.CATALOG_HOLOGRES_ENDPOINT;
import static com.alibaba.ververica.connectors.hologres.jdbc.catalog.HologresCatalogValidator.CATALOG_HOLOGRES_PASSWORD;
import static com.alibaba.ververica.connectors.hologres.jdbc.catalog.HologresCatalogValidator.CATALOG_HOLOGRES_USERNAME;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_DEFAULT_DATABASE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_TYPE;

/** Factory for HologresCatalog. */
public class HologresCatalogFactory implements CatalogFactory {

	@Override
	public Catalog createCatalog(String name, Map<String, String> properties) {
		final DescriptorProperties prop = getValidatedProperties(properties);
		return new HologresCatalog(
				name,
				prop.getString(CATALOG_DEFAULT_DATABASE),
				prop.getString(CATALOG_HOLOGRES_USERNAME),
				prop.getString(CATALOG_HOLOGRES_PASSWORD),
				prop.getString(CATALOG_HOLOGRES_ENDPOINT));
	}

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CATALOG_TYPE, "hologres"); // jdbc
		context.put(CATALOG_PROPERTY_VERSION, "1"); // backwards compatibility
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();
		// default database
		properties.add(CATALOG_DEFAULT_DATABASE);

		properties.add(CATALOG_HOLOGRES_ENDPOINT);
		properties.add(CATALOG_HOLOGRES_USERNAME);
		properties.add(CATALOG_HOLOGRES_PASSWORD);

		return properties;
	}

	private static DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);

		new HologresCatalogValidator().validate(descriptorProperties);

		return descriptorProperties;
	}
}
