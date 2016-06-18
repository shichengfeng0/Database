package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    DbIterator child;
    int afield;
    int gfield;
    Aggregator.Op aop;
    Aggregator aggregator;
    DbIterator itr;
    
    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
    	this.afield = afield;
    	this.gfield = gfield;
    	this.child = child;
    	
    	Type gfieldType = null;
    	if (gfield != Aggregator.NO_GROUPING) {
    		gfieldType = child.getTupleDesc().getFieldType(gfield);
    	}
    	
    	if (child.getTupleDesc().getFieldType(afield).equals(Type.INT_TYPE)) {
			aggregator = new IntegerAggregator(gfield, gfieldType, afield, aop);
		} else {
			aggregator = new StringAggregator(gfield, gfieldType, afield, aop);
		}
		
		try {
			child.open();
			while(child.hasNext()) {
				aggregator.mergeTupleIntoGroup(child.next());
			}
			child.close();
		} catch (NoSuchElementException e) {
			e.printStackTrace();
		} catch (DbException e) {
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			e.printStackTrace();
		}
		
		itr = aggregator.iterator();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
		if (gfield == Aggregator.NO_GROUPING) {
			return Aggregator.NO_GROUPING;
		} else {
			return gfield;
		}
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
		if (gfield != Aggregator.NO_GROUPING) {
			return child.getTupleDesc().getFieldName(gfield);
		} else {
			return null;
		}
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
		return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
    	return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
		return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
    	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
		super.open();
		itr.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		if (itr.hasNext()) {
			return itr.next();
		}
		return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
		itr.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
    	TupleDesc tupleDesc;
    	if(gfield == Aggregator.NO_GROUPING){
    		Type[] typeArr = new Type[1];
    		typeArr[0] = child.getTupleDesc().getFieldType(afield);
    		String[] fieldArr = new String[1]; 
    		fieldArr[0] = child.getTupleDesc().getFieldName(afield);
    		tupleDesc = new TupleDesc(typeArr, fieldArr);
    	}else{
    		Type[] typeArr = new Type[2];
    		System.out.println(child);
    		typeArr[0] = child.getTupleDesc().getFieldType(gfield);
    		typeArr[1] = child.getTupleDesc().getFieldType(afield);
    		String[] fieldArr = new String[2];
    		fieldArr[0] = child.getTupleDesc().getFieldName(gfield);
    		fieldArr[1] = child.getTupleDesc().getFieldName(afield);
    		tupleDesc = new TupleDesc(typeArr, fieldArr);
    	}
		return tupleDesc;
    }

    public void close() {
		super.close();
		itr.close();
    }

    @Override
    public DbIterator[] getChildren() {
    	return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
		child = children[0];
    }
    
}
