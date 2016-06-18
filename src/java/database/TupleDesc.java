package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
	
	ArrayList<TDItem> descArray = new ArrayList<TDItem>();
	
    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
       return descArray.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        for (int i = 0; i < typeAr.length; i++) {
        	descArray.add(new TDItem(typeAr[i], fieldAr[i]));
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
    	this(typeAr, new String[typeAr.length]);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public synchronized int numFields() {
        return descArray.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
    	if (i >= numFields()) {
    		System.out.println("i = " + i);
    		System.out.println("numFields = " + numFields());
    		throw new NoSuchElementException();
    	}
        return descArray.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public synchronized Type getFieldType(int i) throws NoSuchElementException {
    	if (i >= numFields()) {
    		throw new NoSuchElementException();
    	}
    	
        return descArray.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     *         IllegalArgumentException if the name passed in is null
     */
    public synchronized int fieldNameToIndex(String name) throws NoSuchElementException {
    	
        Iterator<TDItem> itr = iterator();
        int i = 0;
        while(itr.hasNext()) {
        	String nextName = itr.next().fieldName;
        	if (nextName == null) continue;
        	if (nextName.equals(name)) {
        		return i;
        	}
        	i++;
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
    	int totalSize = 0;
    	for (TDItem t: descArray) {
    		switch(t.fieldType) {
    			case INT_TYPE: totalSize += Type.INT_TYPE.getLen();
    				break;
    			case STRING_TYPE: totalSize += Type.STRING_TYPE.getLen();
    				break;
    		}
    	}
    	
        return totalSize;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static synchronized TupleDesc merge(TupleDesc td1, TupleDesc td2) {
    	int numFieldsTD1 = td1.numFields();
    	int numFieldsTD2 = td2.numFields();
    	
        Type[] mergeType = new Type[numFieldsTD1 + numFieldsTD2];
        String[] mergeName = new String[numFieldsTD1 + numFieldsTD2];
        for (int i = 0; i < numFieldsTD1; i++) {
        	mergeType[i] = td1.descArray.get(i).fieldType;
        	mergeName[i] = td1.descArray.get(i).fieldName;
        }
        
        for (int i = numFieldsTD1; i < numFieldsTD1 + numFieldsTD2; i++) {
        	mergeType[i] = td2.descArray.get(i - numFieldsTD1).fieldType;
        	mergeName[i] = td2.descArray.get(i - numFieldsTD1).fieldName;
        }
        
        return new TupleDesc(mergeType, mergeName);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the  
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
    	if (!(o instanceof TupleDesc)) {
    		return false;
    	}
    	
    	TupleDesc other = (TupleDesc) o;
    	if (other.numFields() != numFields()) return false;
    	
    	for (int i = 0; i < numFields(); i++) {
    		if (!descArray.get(i).fieldType.equals(other.descArray.get(i).fieldType)) {
    			return false;
    		}
    	}
    	
        return true;
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
        String result = "";
        result += descArray.get(0).fieldType + "(" + descArray.get(0).fieldName + ")";
        
        for (int i = 1; i < numFields(); i++) {
        	result += ", " + descArray.get(i).fieldType + "(" + descArray.get(i).fieldName + ")";
        }
        
        return result;
    }
}
