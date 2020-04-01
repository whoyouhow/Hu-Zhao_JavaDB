package _1_basic;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * This class represents a tuple that will contain a single row's worth of information
 * from a table. It also includes information about where it is stored
 * @author Sam Madden modified by Doug Shook
 *
 */
public class Tuple {
	
	/**
	 * Creates a new tuple with the given description
	 * @param t the schema for this tuple
	 */
	private int pid;
	private int Id;
	private TupleDesc desc;
	private Field[] fields;
	
	public Tuple(TupleDesc t) {
		//your code here
		desc = t;
		fields = new Field[t.numFields()];
	}
	
	public TupleDesc getDesc() {
		//your code here
		return this.desc;
	}
	
	/**
	 * retrieves the page id where this tuple is stored
	 * @return the page id of this tuple
	 */
	public int getPid() {
		//your code here
		return this.pid;
	}

	public void setPid(int pid) {
		//your code here
		this.pid = pid;
	}

	/**
	 * retrieves the tuple (slot) id of this tuple
	 * @return the slot where this tuple is stored
	 */
	public int getId() {
		//your code here
		return this.Id;
	}

	public void setId(int id) {
		//your code here
		this.Id = id;
	}
	
	public void setDesc(TupleDesc td) {
		//your code here;
		this.desc = td;
	}
	
	/**
	 * Stores the given data at the i-th field
	 * @param i the field number to store the data
	 * @param v the data
	 */
	public void setField(int i, Field v) {
		//your code here
		this.fields[i] = v;
		
	}
	
	public Field getField(int i) {
		//your code here
		return (Field)this.fields[i];
	}
	
	/**
	 * Creates a string representation of this tuple that displays its contents.
	 * You should convert the binary data into a readable format (i.e. display the ints in base-10 and convert
	 * the String columns to readable text).
	 */
	public String toString() {
		//your code here
		String str = "";
		for (int i = 0; i < this.desc.numFields(); i++){
			str = str + fields[i].toString();
		}
		return str;
	}
}
	