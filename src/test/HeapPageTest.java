package test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;

import _1_basic.Catalog;
import _1_basic.Database;
import _1_basic.HeapFile;
import _1_basic.HeapPage;
import _1_basic.IntField;
import _1_basic.StringField;
import _1_basic.Tuple;
import _1_basic.TupleDesc;

public class HeapPageTest {
	
	private HeapFile hf;
	private TupleDesc td;
	private Catalog c;
	private HeapPage hp;
	
	@Before
	public void setup() {
		
		try {
			Files.copy(new File("testfiles/test.dat.bak").toPath(), new File("testfiles/test.dat").toPath(), StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			System.out.println("unable to copy files");
			e.printStackTrace();
		}
		
		c = Database.getCatalog();
		c.loadSchema("testfiles/test.txt");
		
		int tableId = c.getTableId("test");
		td = c.getTupleDesc(tableId);
		hf = c.getDbFile(tableId);
		hp = hf.readPage(0);
	}
	
	@Test
	public void testGetters() {
		assertTrue(hp.getNumSlots() == 30);
		assertTrue(hp.slotOccupied(0));
		for(int i = 1; i < 30; i++) {
			assertFalse(hp.slotOccupied(i));
		}
	}
	
	public void testAddTuple() {
		Tuple t = new Tuple(td);
		t.setField(0, new IntField(new byte[] {0, 0, 0, (byte)131}));
		byte[] s = new byte[129];
		s[0] = 2;
		s[1] = 98;
		s[2] = 121;
		t.setField(1, new StringField(s));
		
		try {
			hp.addTuple(t);
		} catch (Exception e) {
			fail("error when adding valid tuple");
			e.printStackTrace();
		}
		
		Iterator<Tuple> it = hp.iterator();
		assertTrue(it.hasNext());
		it.next();
		assertTrue(it.hasNext());
		it.next();
		assertFalse(it.hasNext());
		
	}
	
	@Test
	public void testDelete() {
		Tuple t = new Tuple(td);
		t.setField(0, new IntField(new byte[] {0, 0, (byte)2, (byte)18}));
		byte[] s = new byte[129];
		s[0] = 2;
		s[1] = 0x68;
		s[2] = 0x69;
		t.setField(1, new StringField(s));
		
		try {
			hp.deleteTuple(t);
		} catch (Exception e) {
			fail("error when deleting valid tuple");
			e.printStackTrace();
		}
		
		Iterator<Tuple> it = hp.iterator();
		assertFalse(it.hasNext());
		
		
		
	}

}
