package com.alibaba.hologres.client.model;

import com.alibaba.hologres.client.exception.InvalidIdentifierException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * TableName Tester.
 *
 * @version 1.0
 * @since <pre>12月 2, 2020</pre>
 */
public class TableNameTest {

	@Before
	public void before() throws Exception {
	}

	@After
	public void after() throws Exception {
	}

	/**
	 * Method: valueOf(String schemaName, String tableName).
	 */
	@Test
	public void testValueOfForSchemaNameTableName() throws Exception {
		//TODO: Test goes here...
	}


	/**
	 * 无引号，小写，无非法字符.
	 * Method: valueOf(String name).
	 */
	@Test
	public void testValueOfName001() throws Exception {
		TableName tn = TableName.valueOf("aaa");
		Assert.assertEquals("public", tn.getSchemaName());
		Assert.assertEquals("aaa", tn.getTableName());
		Assert.assertEquals("\"public\".\"aaa\"", tn.getFullName());
	}

	/**
	 * 无引号，小写，有非法字符.
	 * Method: valueOf(String name).
	 */
	@Test
	public void testValueOfName002() throws Exception {
		Assert.assertThrows(InvalidIdentifierException.class, () -> {
			TableName tn = TableName.valueOf("a aa");
		});
	}


	/**
	 * 无引号，大写，无非法字符.
	 * Method: valueOf(String name).
	 */
	@Test
	public void testValueOfName003() throws Exception {
		TableName tn = TableName.valueOf("aAa");
		Assert.assertEquals("public", tn.getSchemaName());
		Assert.assertEquals("aaa", tn.getTableName());
		Assert.assertEquals("\"public\".\"aaa\"", tn.getFullName());
	}

	/**
	 * 无引号，大写，有非法字符.
	 * Method: valueOf(String name).
	 */
	@Test
	public void testValueOfName004() throws Exception {
		Assert.assertThrows(InvalidIdentifierException.class, () -> {
			TableName tn = TableName.valueOf("a Aa");
		});
	}

	/**
	 * 无引号，小写，有非法字符.
	 * Method: valueOf(String name).
	 */
	@Test
	public void testValueOfName005() throws Exception {
		Assert.assertThrows(InvalidIdentifierException.class, () -> {
			TableName tn = TableName.valueOf("a\"aa");
		});
	}

	/**
	 * 有引号，小写，无特殊字符.
	 * Method: valueOf(String name).
	 */
	@Test
	public void testValueOfName006() throws Exception {
		TableName tn = TableName.valueOf("\"aaa\"");
		Assert.assertEquals("public", tn.getSchemaName());
		Assert.assertEquals("aaa", tn.getTableName());
		Assert.assertEquals("\"public\".\"aaa\"", tn.getFullName());
	}

	/**
	 * 有引号，小写，有特殊字符.
	 * Method: valueOf(String name).
	 */
	@Test
	public void testValueOfName007() throws Exception {
		TableName tn = TableName.valueOf("\"a aa\"");
		Assert.assertEquals("public", tn.getSchemaName());
		Assert.assertEquals("a aa", tn.getTableName());
		Assert.assertEquals("\"public\".\"a aa\"", tn.getFullName());

	}

	/**
	 * 有引号，大写，有特殊字符.
	 * Method: valueOf(String name).
	 */
	@Test
	public void testValueOfName008() throws Exception {
		TableName tn = TableName.valueOf("\"a\"\"A\"");
		Assert.assertEquals("public", tn.getSchemaName());
		Assert.assertEquals("a\"A", tn.getTableName());
		Assert.assertEquals("\"public\".\"a\"\"A\"", tn.getFullName());

	}

	/**
	 * Method: equals(Object o).
	 */
	@Test
	public void testEquals() throws Exception {
		//TODO: Test goes here...
	}
}
