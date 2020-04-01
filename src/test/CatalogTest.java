package test;

import static org.junit.Assert.*;

import java.util.NoSuchElementException;

import org.junit.Test;

import _1_basic.Catalog;
import _1_basic.TupleDesc;
import _1_basic.Type;

public class CatalogTest {

	
	@Test
	public void test() {
		Catalog c = new Catalog();
		System.out.println(System.getProperty("user.dir"));
		c.loadSchema("testfiles/test.txt");
		
		int tableId = c.getTableId("test");
				
		TupleDesc td = new TupleDesc(new Type[] { Type.INT, Type.STRING}, new String[] {"c1", "c2"});
		assertTrue(c.getTupleDesc(tableId).equals(td));
		assertTrue(c.getTableName(tableId).equals("test"));
		try {
			c.getDbFile(tableId);
		} catch(NoSuchElementException e) {
			fail("HeapFile not found");
		}
	}

}
