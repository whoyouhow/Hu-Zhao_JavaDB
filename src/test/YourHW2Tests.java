package test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;

import _1_basic.Catalog;
import _1_basic.Database;
import _1_basic.HeapFile;
import _1_basic.IntField;
import _1_basic.RelationalOperator;
import _1_basic.Tuple;
import _1_basic.TupleDesc;
import _2_query.*;

public class YourHW2Tests {

	private HeapFile testhf;
	private TupleDesc testtd;
	private HeapFile ahf;
	private TupleDesc atd;
	private Catalog c;

	@Before
	public void setup() {
		
		try {
			Files.copy(new File("testfiles/test.dat.bak").toPath(), new File("testfiles/test.dat").toPath(), StandardCopyOption.REPLACE_EXISTING);
			Files.copy(new File("testfiles/A.dat.bak").toPath(), new File("testfiles/A.dat").toPath(), StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			System.out.println("unable to copy files");
			e.printStackTrace();
		}
		
		c = Database.getCatalog();
		c.loadSchema("testfiles/test.txt");
		
		int tableId = c.getTableId("test");
		testtd = c.getTupleDesc(tableId);
		testhf = c.getDbFile(tableId);
		
		c = Database.getCatalog();
		c.loadSchema("testfiles/A.txt");
		
		tableId = c.getTableId("A");
		atd = c.getTupleDesc(tableId);
		ahf = c.getDbFile(tableId);
//		System.out.println(testhf.getTupleDesc().toString());
//		System.out.println(ahf.getTupleDesc().toString());
//		System.out.println("test file");
//		testhf.getAllTuples();
//		System.out.println("A file");
//		ahf.getAllTuples();
//		System.out.println("end of before");
	}
	
//	Added table : test with schema int(c1),String(c2)
//	Added table : A with schema int(a1),int(a2)
//	test file
//	int(c1),String(c2)
//	0 530 hi
//	A file
//	int(a1),int(a2)
//	0 530 1
//	1 1 2
//	2 530 3
//	3 530 4
//	4 530 5
//	5 2 6
//	6 530 7
//	7 3 8
	
//	@Test
//	public void test() {
//		fail("Not yet implemented");
//	}
	@Test
	public void testSelect2() {
		System.out.println("select");
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		ar = ar.select(0, RelationalOperator.EQ, new IntField(530));
		
		assertTrue("Should be 5 tuples after select operation", ar.getTuples().size() == 5);
		assertTrue("select operation does not change tuple description", ar.getDesc().equals(atd));
		
		ar = ar.select(1, RelationalOperator.EQ, new IntField(4));
		assertTrue("Should be 1 tuple after select operation", ar.getTuples().size() == 1);
		assertTrue("select operation does not change tuple description", ar.getDesc().equals(atd));
		
		Relation ar2 = new Relation(ahf.getAllTuples(), atd);
		ar2 = ar2.select(0, RelationalOperator.EQ, new IntField(1));
		assertTrue("Should be 1 tuple after select operation", ar2.getTuples().size() == 1);
	}

	@Test
	public void testProject() {
		System.out.println("project");
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		ArrayList<Integer> c = new ArrayList<Integer>();
		c.add(1);
		ar = ar.project(c);
		assertTrue("Projection should remove one of the columns, making the size smaller", ar.getDesc().getSize() == 4);
		assertTrue("Projection should not change the number of tuples", ar.getTuples().size() == 8);
		assertTrue("Projection should retain the column names", ar.getDesc().getFieldName(0).equals("a2"));
	}
	@Test
	public void testProject2() {
		System.out.println("project");
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		ArrayList<Integer> c = new ArrayList<Integer>();
		c.add(0);
		ar = ar.project(c);
		assertTrue("Projection should remove one of the columns, making the size smaller", ar.getDesc().getSize() == 4);
		assertTrue("Projection should not change the number of tuples", ar.getTuples().size() == 8);
		assertTrue("Projection should retain the column names", ar.getDesc().getFieldName(0).equals("a1"));
	}
	@Test
	public void testProject3() {
		System.out.println("project");
		Relation ar = new Relation(testhf.getAllTuples(), testtd);
		ArrayList<Integer> c = new ArrayList<Integer>();
		c.add(1);
		ar = ar.project(c);
		assertTrue("Projection should remove one of the columns, making the size smaller", ar.getDesc().getSize() == 129);
		assertTrue("Projection should not change the number of tuples", ar.getTuples().size() == 1);
		assertTrue("Projection should retain the column names", ar.getDesc().getFieldName(0).equals("c2"));
	}

	@Test
	public void testJoin() {
		System.out.println("join");
		Relation tr = new Relation(testhf.getAllTuples(), testtd);
		System.out.println(tr.getTuples().size());
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		tr = tr.join(ar, 0, 0);
		System.out.println(tr.getTuples().size());
		
		assertTrue("There should be 5 tuples after the join", tr.getTuples().size() == 5);
		assertTrue("The size of the tuples should reflect the additional columns from the join", tr.getDesc().getSize() == 141);
	}
	@Test
	public void testJoin2() {//also tests select
		System.out.println("join");
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		ar = ar.select(1, RelationalOperator.EQ, new IntField(1));
		Relation tr = new Relation(testhf.getAllTuples(), testtd);
		System.out.println(tr.getTuples().size());
		tr = tr.join(ar, 0, 0);
		System.out.println(tr.getTuples().size());
		
		assertTrue("There should be 5 tuples after the join", tr.getTuples().size() == 1);
		assertTrue("The size of the tuples should reflect the additional columns from the join", tr.getDesc().getSize() == 141);
	}
	
	@Test
	public void testRename() {
		System.out.println("rename");
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		
		ArrayList<Integer> f = new ArrayList<Integer>();
		ArrayList<String> n = new ArrayList<String>();
		
		f.add(0);
		n.add("b1");
		
		try {
			ar = ar.rename(f, n);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		assertTrue("Rename should not remove any tuples", ar.getTuples().size() == 8);
		assertTrue("Rename did not go through", ar.getDesc().getFieldName(0).equals("b1"));
		assertTrue("Rename changed the wrong column", ar.getDesc().getFieldName(1).equals("a2"));
		assertTrue("Rename should not add or remove any columns", ar.getDesc().getSize() == 8);
		
	}
	public void testRename2() {
		System.out.println("rename");
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		
		ArrayList<Integer> f = new ArrayList<Integer>();
		ArrayList<String> n = new ArrayList<String>();
		
		f.add(1);
		n.add("b2");
		
		try {
			ar = ar.rename(f, n);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		assertTrue("Rename should not remove any tuples", ar.getTuples().size() == 8);
		assertTrue("Rename did not go through", ar.getDesc().getFieldName(1).equals("b2"));
		assertTrue("Rename changed the wrong column", ar.getDesc().getFieldName(0).equals("a1"));
		assertTrue("Rename should not add or remove any columns", ar.getDesc().getSize() == 8);
		
	}
	public void testRename3() {
		System.out.println("rename");
		Relation tr = new Relation(testhf.getAllTuples(), testtd);
		
		ArrayList<Integer> f = new ArrayList<Integer>();
		ArrayList<String> n = new ArrayList<String>();
		
		f.add(0);
		n.add("b1");
		
		try {
			tr = tr.rename(f, n);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		assertTrue("Rename should not remove any tuples", tr.getTuples().size() == 133);
		assertTrue("Rename did not go through", tr.getDesc().getFieldName(0).equals("b1"));
		assertTrue("Rename changed the wrong column", tr.getDesc().getFieldName(1).equals("c2"));
		assertTrue("Rename should not add or remove any columns", tr.getDesc().getSize() == 133);
		
	}

	@Test
	public void testAggregate() {
		System.out.println("aggregate");
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		ArrayList<Integer> c = new ArrayList<Integer>();
		c.add(1);
		ar = ar.project(c);
		ar = ar.aggregate(AggregateOperator.SUM, false);
//		ArrayList<Tuple> ts=ar.getTuples();
//		System.out.println("length:"+ts.size());
//		for(Tuple t:ts) {
//		System.out.println(t.toString());}
		
		assertTrue("The result of an aggregate should be a single tuple", ar.getTuples().size() == 1);
		IntField agg = (IntField) ar.getTuples().get(0).getField(0);
		assertTrue("The sum of these values was incorrect", agg.getValue() == 36);
	}
	@Test
	public void testAggregate2() {
		System.out.println("aggregate");
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		ArrayList<Integer> c = new ArrayList<Integer>();
		c.add(0);//aggregate field a1
		ar = ar.project(c);
		ar = ar.aggregate(AggregateOperator.SUM, false);
		
		assertTrue("The result of an aggregate should be a single tuple", ar.getTuples().size() == 1);
		IntField agg = (IntField) ar.getTuples().get(0).getField(0);
//		System.out.println(agg.getValue());
		assertTrue("The sum of these values was incorrect", agg.getValue() == 2656);//530*5+1+2+3
	}
//	@Test
//	public void testAggregate3() {
//		System.out.println("aggregate");
//		Relation ar = new Relation(ahf.getAllTuples(), atd);
//		ArrayList<Integer> c = new ArrayList<Integer>();
//		c.add(1);
//		ar = ar.project(c);
//		ar = ar.aggregate(AggregateOperator.AVG, false);
////		ArrayList<Tuple> ts=ar.getTuples();
////		System.out.println("length:"+ts.size());
////		for(Tuple t:ts) {
////		System.out.println(t.toString());}
//		
//		assertTrue("The result of an aggregate should be a single tuple", ar.getTuples().size() == 1);
//		IntField agg = (IntField) ar.getTuples().get(0).getField(0);
//		assertTrue("The sum of these values was incorrect", agg.getValue() == 4);
//	}

	@Test
	public void testGroupBy() {
		System.out.println("groupby");
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		ar = ar.aggregate(AggregateOperator.SUM, true);
		assertTrue("There should be four tuples after the grouping", ar.getTuples().size() == 4);
		assertTrue("The result where a1==530 should be 20", ar.getTuples().get(0).getField(1).toString().equals("20"));
	}
	@Test
	public void testGroupBy2() {//also tests select
		System.out.println("groupby");
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		ar = ar.select(1, RelationalOperator.NOTEQ, new IntField(3));
		ar = ar.aggregate(AggregateOperator.SUM, true);
		assertTrue("There should be four tuples after the grouping", ar.getTuples().size() == 4);
		assertTrue("The result where a1==530 should be 17", ar.getTuples().get(0).getField(1).toString().equals("17"));
	}
	
	
	
	/////////////////////////
	
	

	@Test
	public void testSimpleQ() {
		Query q = new Query("SELECT a1, a2 FROM A");
		Relation r = q.execute();
		
		assertTrue("Select should not change the number of tuples", r.getTuples().size() == 8);
		assertTrue("Query does not add or remove column", r.getDesc().getSize() == 8);
	}
	@Test
	public void testSimpleQ2() {
		Query q = new Query("SELECT a1 FROM A");
		Relation r = q.execute();
		
		assertTrue("Select should not change the number of tuples", r.getTuples().size() == 8);
		assertTrue("Query does not add or remove column", r.getDesc().getSize() == 4);
	}

	@Test
	public void testSelectQ() {
		Query q = new Query("SELECT a1, a2 FROM A WHERE a1 = 530");
		Relation r = q.execute();
		
		assertTrue("Result of query should contain 5 tuples", r.getTuples().size() == 5);
		assertTrue("Where clause does not add or remove columns", r.getDesc().getSize() == 8);
	}
	@Test
	public void testSelectQ2() {
		Query q = new Query("SELECT a2 FROM A WHERE a1 = 530");
		Relation r = q.execute();
		
		assertTrue("Result of query should contain 5 tuples", r.getTuples().size() == 5);
		assertTrue("Where clause does not add or remove columns", r.getDesc().getSize() == 4);
	}
	@Test
	public void testSelectQ3() {
		Query q = new Query("SELECT a1, a2 FROM A WHERE a1 = 1");
		Relation r = q.execute();
		
		assertTrue("Result of query should contain 1 tuple", r.getTuples().size() == 1);
		assertTrue("Where clause does not add or remove columns", r.getDesc().getSize() == 8);
	}

	@Test
	public void testProjectQ() {
		Query q = new Query("SELECT a2 FROM A");
		Relation r = q.execute();
		
		assertTrue("Projection should remove a column", r.getDesc().getSize() == 4);
		assertTrue("Projection should not remove tuples", r.getTuples().size() == 8);
		assertTrue("Projection removed the wrong column", r.getDesc().getFieldName(0).equals("a2"));
	}
	@Test
	public void testProjectQ2() {
		Query q = new Query("SELECT a1 FROM A");
		Relation r = q.execute();
		
		assertTrue("Projection should remove a column", r.getDesc().getSize() == 4);
		assertTrue("Projection should not remove tuples", r.getTuples().size() == 8);
		assertTrue("Projection removed the wrong column", r.getDesc().getFieldName(0).equals("a1"));
	}

	@Test
	public void testJoinQ() {
		Query q = new Query("SELECT c1, c2, a1, a2 FROM test JOIN A ON test.c1 = a.a1");
		Relation r = q.execute();
		
		assertTrue("Join should return 5 tuples", r.getTuples().size() == 5);
		assertTrue("Tuple size should increase since columns were added with join", r.getDesc().getSize() == 141);
	}
	@Test
	public void testJoinQ2() {
		Query q = new Query("SELECT c1, c2, a1 FROM test JOIN A ON test.c1 = a.a1");
		Relation r = q.execute();
		
		assertTrue("Join should return 5 tuples", r.getTuples().size() == 5);
		assertTrue("Tuple size should increase since columns were added with join", r.getDesc().getSize() == 137);
	}
	@Test
	public void testJoinQ3() {
		Query q = new Query("SELECT c1, c2, a1, a2 FROM test JOIN A ON test.c1 = a.a2");
		Relation r = q.execute();
		assertTrue("Join should return 0 tuple", r.getTuples().size() == 0);
		assertTrue("Tuple size should increase since columns were added with join", r.getDesc().getSize() == 141);
	}

	@Test
	public void testAggregateQ() {
		Query q = new Query("SELECT SUM(a2) FROM A");
		Relation r = q.execute();
		
		assertTrue("Aggregations should result in one tuple",r.getTuples().size() == 1);
		IntField agg = (IntField) r.getTuples().get(0).getField(0);
		assertTrue("Result of sum aggregation is 36", agg.getValue() == 36);
	}
//	@Test
//	public void testAggregateQ2() {
//		Query q = new Query("SELECT SUM(a2) FROM A");
//		Relation r = q.execute();
//		
//		assertTrue("Aggregations should result in one tuple",r.getTuples().size() == 1);
//		IntField agg = (IntField) r.getTuples().get(0).getField(0);
//		assertTrue("Result of sum aggregation is 36", agg.getValue() == 4);
//	}
	
	@Test
	public void testGroupByQ() {
		Query q = new Query("SELECT a1, SUM(a2) FROM A GROUP BY a1");
		Relation r = q.execute();
		
		assertTrue("Should be 4 groups from this query", r.getTuples().size() == 4);
	}
	
	@Test
	public void testSelectAllQ() {
		Query q = new Query("SELECT * FROM A");
		Relation r = q.execute();
		
		assertTrue("should return all 8 tuples", r.getTuples().size() == 8);
		assertTrue("number of columns should be unchanged", r.getDesc().getSize() == 8);
	}
	


}
