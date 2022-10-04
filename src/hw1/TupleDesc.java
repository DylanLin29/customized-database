package hw1;

import java.util.*;

import org.hamcrest.core.IsInstanceOf;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc {

	private Type[] types; // datatypes that the tuple contains (only contains INT and STRING in this case)
	private String[] fields;

	/**
	 * Create a new TupleDesc with typeAr.length fields with fields of the
	 * specified types, with associated named fields.
	 *
	 * @param typeAr  array specifying the number of and types of fields in
	 *                this TupleDesc. It must contain at least one entry.
	 * @param fieldAr array specifying the names of the fields. Note that names may
	 *                be null.
	 */
	public TupleDesc(Type[] typeAr, String[] fieldAr) {
		// your code here
		fields = fieldAr.clone();
		types = typeAr.clone();
	}

	/**
	 * @return the number of fields in this TupleDesc
	 */
	public int numFields() {
		// your code here
		return this.fields.length;
	}

	/**
	 * Gets the (possibly null) field name of the ith field of this TupleDesc.
	 *
	 * @param i index of the field name to return. It must be a valid index.
	 * @return the name of the ith field
	 * @throws NoSuchElementException if i is not a valid field reference.
	 */
	public String getFieldName(int i) throws NoSuchElementException {
		// your code here
		// when i is not a valid field reference
		if (i >= numFields() || i < 0) {
			throw new NoSuchElementException();
		}
		return this.fields[i];
	}

	/**
	 * Find the index of the field with a given name.
	 *
	 * @param name name of the field.
	 * @return the index of the field that is first to have the given name.
	 * @throws NoSuchElementException if no field with a matching name is found.
	 */
	public int nameToId(String name) throws NoSuchElementException {
		// your code here
		for (int i = 0; i < this.numFields(); i++) {
			if (this.fields[i] == name) {
				return i;
			}
		}
		// when no field is found
		throw new NoSuchElementException();
	}

	/**
	 * Gets the type of the ith field of this TupleDesc.
	 *
	 * @param i The index of the field to get the type of. It must be a valid index.
	 * @return the type of the ith field
	 * @throws NoSuchElementException if i is not a valid field reference.
	 */
	public Type getType(int i) throws NoSuchElementException {
		// your code here
		if (i >= this.types.length || i < 0) {
			throw new NoSuchElementException();
		}
		return this.types[i];
	}

	/**
	 * @return The size (in bytes) of tuples corresponding to this TupleDesc.
	 *         Note that tuples from a given TupleDesc are of a fixed size.
	 */
	public int getSize() {
		// your code here
		int size = 0;
		for (Type type : this.types) {
			if (type == Type.INT) {
				size += 4;
			} else {
				// Type.String
				size += 129;
			}
		}
		return size;
	}

	/**
	 * Compares the specified object with this TupleDesc for equality.
	 * Two TupleDescs are considered equal if they are the same size and if the
	 * n-th type in this TupleDesc is equal to the n-th type in td.
	 *
	 * @param o the Object to be compared for equality with this TupleDesc.
	 * @return true if the object is equal to this TupleDesc.
	 */
	public boolean equals(Object o) {
		// your code here
		if (o instanceof TupleDesc) {
			TupleDesc tupleDesc = (TupleDesc) o;
			if (tupleDesc.getSize() != this.getSize()) {
				return false;
			}
			if (tupleDesc.numFields() != this.numFields()) {
				return false;
			}
			for (int i = 0; i < this.numFields(); i++) {
				if (tupleDesc.getType(i) != this.getType(i)) {
					return false;
				}
			}
			return true;
		}
		return false;
	}

	public int hashCode() {
		// If you want to use TupleDesc as keys for HashMap, implement this so
		// that equal objects have equals hashCode() results
		throw new UnsupportedOperationException("unimplemented");
	}

	/**
	 * Returns a String describing this descriptor. It should be of the form
	 * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
	 * the exact format does not matter.
	 * 
	 * @return String describing this descriptor.
	 */
	public String toString() {
		// your code here
		StringBuilder str = new StringBuilder();
		for (int i = 0; i < this.fields.length; i++) {
			str.append("fieldType[" + i + "](" + this.fields[i] + "), ");
		}
		int length = str.length();
		if (length > 0) {
			str.setLength(length - 2);
		}
		return str.toString();
	}
}
