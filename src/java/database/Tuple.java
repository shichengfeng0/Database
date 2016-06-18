package simpledb;

import java.io.Serializable;
import java.util.ArrayList;

import java.util.Iterator;


/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {
	TupleDesc tupleDesc;
	RecordId recordId;
	
	ArrayList<Field> fieldsList = new ArrayList<Field>();
	
    private static final long serialVersionUID = 1L;

    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
       tupleDesc = td;
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
    	recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     * 
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
    	//check whether the passed in field could be insert in to index i
    	//throw IllegalArgumentException if not
    	if (!tupleDesc.descArray.get(i).fieldType.equals(f.getType())) {
    		throw new IllegalArgumentException("Incorrect type");
    	}
    	fieldsList.add(i, f);
    }
    
    
    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        return fieldsList.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
    	String result = "";
    	result += fieldsList.get(0).toString();
    	
    	for (int i = 1; i < fieldsList.size(); i++) {
    		result += "\t" + fieldsList.get(i).toString();
    	}
    	
    	result += "\n";
        return result;
    }
    
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields() {
        return fieldsList.iterator();
    }
    
    /**
     * reset the TupleDesc of thi tuple
     * */
    public void resetTupleDesc(TupleDesc td) {
    	tupleDesc = td;
    }
    
    public boolean equals(Object other) {
    	if (other instanceof Tuple) {
    		Tuple otherTuple = (Tuple) other;
    		ArrayList<Field> otherList = otherTuple.fieldsList;
    		if (fieldsList.size() != otherList.size()) {
    			return false;
    		}
    		for (int i = 0; i < fieldsList.size(); i++) {
    			if (!fieldsList.get(i).equals(otherList.get(i))) {
    				return false;
    			}
    		}
    		return true;
    	}
    	return false;
    }
}
