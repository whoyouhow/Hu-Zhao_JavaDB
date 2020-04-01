package _2_query;

import static org.junit.Assert.assertNotNull;

import java.awt.print.Printable;
import java.util.ArrayList;
import java.util.Iterator;

import javax.naming.InitialContext;

import _1_basic.Field;
import _1_basic.RelationalOperator;
import _1_basic.Tuple;
import _1_basic.TupleDesc;
import _1_basic.Type;

/**
 * This class provides methods to perform relational algebra operations. It will be used
 * to implement SQL queries.
 * @author Doug Shook
 *
 */
public class Relation {

	private ArrayList<Tuple> tuples;
	private TupleDesc td;
	
	public Relation(ArrayList<Tuple> l, TupleDesc td) {
		//your code here
		this.tuples = l;
		this.td = td;
	}
	
	/**
	 * This method performs a select operation on a relation
	 * @param field number (refer to TupleDesc) of the field to be compared, left side of comparison
	 * @param op the comparison operator
	 * @param operand a constant to be compared against the given column
	 * @return
	 */
	public Relation select(int field, RelationalOperator op, Field operand) {
		//your code here
		ArrayList<Tuple> selected = new ArrayList<Tuple>();
		Tuple t = null;
		Iterator<Tuple> iterator = this.getTuples().iterator();
		while (iterator.hasNext()){
			t = iterator.next();
			if (t.getField(field).compare(op, operand))
				selected.add(t);
		}
		Relation newRelation = new Relation(selected, this.td);
		return newRelation;
	}
	
	/**
	 * This method performs a rename operation on a relation
	 * @param fields the field numbers (refer to TupleDesc) of the fields to be renamed
	 * @param names a list of new names. The order of these names is the same as the order of field numbers in the field list
	 * @return
	 * @throws Exception 
	 */
	public Relation rename(ArrayList<Integer> fields, ArrayList<String> names) throws Exception {
		//your code here
		if (!(names.size() == 1 && names.get(0).equals(""))) {
			for(int i = 0; i < names.size(); i++) {
			    if(this.td.contains(names.get(i))) {
			     throw new Exception();
			    }
			   }
			Type[] type = new Type[this.getDesc().numFields()];
			String[] string = new String[this.getDesc().numFields()];
			TupleDesc tD = this.getDesc();
			for (int i = 0; i < tD.numFields(); i++){
				type[i] = tD.getType(i);
				string[i] = tD.getFieldName(i);
			}

			for (int i = 0; i < fields.size(); i++){
				string[fields.get(i)] = names.get(i);
			}
			TupleDesc newDesc = new TupleDesc(type, string);
			return new Relation(this.getTuples(), newDesc);
		}else {
			return this;
		}
		
	}
	
	/**
	 * This method performs a project operation on a relation
	 * @param fields a list of field numbers (refer to TupleDesc) that should be in the result
	 * @return
	 */
	public Relation project(ArrayList<Integer> fields) {
		//your code here
		if (fields.size()!=0) {
			ArrayList<Type> type = new ArrayList<Type>();
			ArrayList<String> string = new ArrayList<String>();
			TupleDesc tD = this.getDesc();
			for (int i = 0; i < tD.numFields(); i++){
				type.add(tD.getType(i));
				string.add(tD.getFieldName(i));
			}
			Type[] newType = new Type[fields.size()];
			String[] newString = new String[fields.size()];
			Iterator<Integer> iterator = fields.iterator();
			int temp = 0;
			while (iterator.hasNext()){
				int index = iterator.next();
				if (index > this.getDesc().numFields() - 1 || index < 0)
					throw new IllegalArgumentException();
				newType[temp] = type.get(index);
				newString[temp] = string.get(index);
				temp = temp + 1;
			}
			TupleDesc newDesc = new TupleDesc(newType, newString);
			ArrayList<Tuple> newTuples = new ArrayList<Tuple>();
			for(Tuple t: this.tuples) {
				Tuple newTuple = new Tuple(newDesc);
				for(int i=0; i < fields.size(); i++) {
					newTuple.setField(i, t.getField(fields.get(i)));
				}
				newTuples.add(newTuple);
			}
			this.tuples = newTuples;
			return new Relation(this.getTuples(), newDesc);
		}else {
			Type[] typesTemp = {};
			String[] fieldsTemp = {};
			return new Relation(new ArrayList<Tuple>(), new TupleDesc(typesTemp, fieldsTemp));
		}
	}
	
	/**
	 * This method performs a join between this relation and a second relation.
	 * The resulting relation will contain all of the columns from both of the given relations,
	 * joined using the equality operator (=)
	 * @param other the relation to be joined
	 * @param field1 the field number (refer to TupleDesc) from this relation to be used in the join condition
	 * @param field2 the field number (refer to TupleDesc) from other to be used in the join condition
	 * @return
	 */
	public Relation join(Relation other, int field1, int field2) {
		//your code here
		ArrayList<Tuple> tuples1 = this.getTuples();
		ArrayList<Tuple> tuples2 = other.getTuples();
		ArrayList<Tuple> tuples = new ArrayList<Tuple>();
		TupleDesc td1 = this.getDesc();
		TupleDesc td2 = other.getDesc();
		
		Type[] type1 = new Type[td1.numFields()];
		Type[] type2 = new Type[td2.numFields()];
		String[] string1 = new String[td1.numFields()];
		String[] string2 = new String[td2.numFields()];
		Type[] type = new Type[td1.numFields()+td2.numFields()];
		String[] string = new String[td1.numFields()+td2.numFields()];
		for (int i = 0; i < td1.numFields(); i++){
			type1[i] = td1.getType(i);
			string1[i] = td1.getFieldName(i);
			type[i] = type1[i];
			string[i] = string1[i];
		}
		for (int i = 0; i < td2.numFields(); i++){
			type2[i] = td2.getType(i);
			string2[i] = td2.getFieldName(i);
			type[i+td1.numFields()] = type2[i];
			string[i+td1.numFields()] = string2[i];
		}
		TupleDesc td12 = new TupleDesc(type, string);
		Iterator<Tuple> iterator1 = tuples1.iterator();
		while (iterator1.hasNext()){
			Tuple tuple1 = iterator1.next();
			Iterator<Tuple> iterator2 = tuples2.iterator();
			while (iterator2.hasNext()){
				Tuple tuple2 = iterator2.next();
				if (tuple1.getField(field1).equals(tuple2.getField(field2))){
					Field[] fields = new Field[td1.numFields()+td2.numFields()];
					Tuple tuple = new Tuple(td12);
					for (int i = 0; i < td1.numFields(); i++){
						fields[i] = tuple1.getField(i);
						tuple.setField(i, fields[i]);
					}
					for (int i = 0; i < td2.numFields(); i++){
						fields[i+td1.numFields()] = tuple2.getField(i);
						tuple.setField(i+td1.numFields(), fields[i+td1.numFields()]);
					}
					
					tuples.add(tuple);
				}
			}
		}
		return new Relation(tuples, td12);
	}
	
	/**
	 * Performs an aggregation operation on a relation. See the lab write up for details.
	 * @param op the aggregation operation to be performed
	 * @param groupBy whether or not a grouping should be performed
	 * @return
	 */
	public Relation aggregate(AggregateOperator op, boolean groupBy) {
		//your code here
		Aggregator aggregator = new Aggregator(op, groupBy, this.getDesc());
		for (int i = 0; i < this.getTuples().size(); i ++) {
			aggregator.merge(this.getTuples().get(i));
		}
//		aggregator.merge(this.getTuples().get(2));
		return new Relation(aggregator.getResults(), this.getDesc());
	}
	
	public TupleDesc getDesc() {
		//your code here
		return this.td;
	}
	
	public ArrayList<Tuple> getTuples() {
		//your code here
		return this.tuples;
	}
	
	/**
	 * Returns a string representation of this relation. The string representation should
	 * first contain the TupleDesc, followed by each of the tuples in this relation
	 */
	public String toString() {
		//your code here
		String string = null;
		string = string + this.getDesc().toString();
		for (int i = 0; i < this.tuples.size(); i++) {
			string = string + tuples.get(i).toString();
		}
		return string;
	}
}
