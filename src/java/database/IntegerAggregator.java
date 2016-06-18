package simpledb;

import java.util.ArrayList;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    int gbfield;
    Type gbfieldtype;
    int afield;
    Op what;
    ArrayList<GbfieldAndAfield> groupMap;
    
    
    
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    	
       this.gbfield = gbfield;
       this.gbfieldtype = gbfieldtype;
       this.afield = afield;
       this.what = what;
       groupMap = new ArrayList<GbfieldAndAfield>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	if (gbfield == Aggregator.NO_GROUPING) {
    		//if there is no grouping, add the aggregateValue in to the groupMap directly
    		ArrayList<Field> afieldlist = new ArrayList<Field>();
    		afieldlist.add(tup.getField(afield));
    		groupMap.add(new GbfieldAndAfield(null, afieldlist));
    	} else {
    		// if there does exist grouping
	        Field comparedField = tup.getField(gbfield);
	        for (int i = 0; i < groupMap.size(); i++) {
	        	GbfieldAndAfield gbfa = groupMap.get(i);
	        	if (gbfa.getGbField().equals(comparedField)) {
	        		gbfa.getAggregateList().add(tup.getField(afield));
	        		return;
	        	}
	        }
	        
	        //if the group map do not contains the comparedField
	        ArrayList<Field> afieldlist = new ArrayList<Field>();
	        afieldlist.add(tup.getField(afield));
	        groupMap.add(new GbfieldAndAfield(comparedField, afieldlist));
    	}
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
    	return new IntegerAggregatorIterator(what, groupMap, gbfieldtype);
    }

}
