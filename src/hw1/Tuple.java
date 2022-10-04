package hw1;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * This class represents a tuple that will contain a single row's worth of
 * information
 * from a table. It also includes information about where it is stored
 * 
 * @author Sam Madden modified by Doug Shook
 *
 */
public class Tuple {

	private TupleDesc tupleDesc;
	private Field[] values;
	private int pid;
	private int id;

	/**
	 * Creates a new tuple with the given description
	 * 
	 * @param t the schema for this tuple
	 */
	public Tuple(TupleDesc t) {
		// your code here
		this.tupleDesc = t;
		values = new Field[t.numFields()];
	}

	public TupleDesc getDesc() {
		// your code here
		return this.tupleDesc;
	}

	/**
	 * retrieves the page id where this tuple is stored
	 * 
	 * @return the page id of this tuple
	 */
	public int getPid() {
		return this.pid;
	}

	public void setPid(int pid) {
		this.pid = pid;
	}

	/**
	 * retrieves the tuple (slot) id of this tuple
	 * 
	 * @return the slot where this tuple is stored
	 */
	public int getId() {
		return this.id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public void setDesc(TupleDesc td) {
		// your code here;
		this.tupleDesc = td;
		this.values = new Field[td.numFields()];
	}

	/**
	 * Stores the given data at the i-th field
	 * 
	 * @param i the field number to store the data
	 * @param v the data
	 */
	public void setField(int i, Field v) {
		// your code here
		this.values[i] = v;
	}

	public Field getField(int i) {
		// your code here
		return this.values[i];
	}

	/**
	 * Creates a string representation of this tuple that displays its contents.
	 * You should convert the binary data into a readable format (i.e. display the
	 * ints in base-10 and convert
	 * the String columns to readable text).
	 */
	public String toString() {
		// your code here
		StringBuilder sb = new StringBuilder();
		for (Field value : values) {
			sb.append(value.toString() + ", ");
		}
		if (sb.length() > 0) {
			sb.setLength(sb.length() - 2);
		}
		return sb.toString();
	}
}
