package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    
    DbIterator child1, child2;
    JoinPredicate p;
    Queue<Tuple> tmpTuplesList;
    ArrayList<Tuple> list = new ArrayList<Tuple>();
    Map<Field, ArrayList<Tuple>> hashMap; //used for hash equal join
    
    
    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        this.p = p;
        this.child1 = child1;
        this.child2 = child2;
        tmpTuplesList = new LinkedList<Tuple>();
        hashMap = new HashMap<Field, ArrayList<Tuple>>();
        
        try {
			child2.open();
			while (child2.hasNext()) {
				Tuple newTuple = child2.next();
				Field field2 = newTuple.getField(p.getField2());
				if (hashMap.containsKey(field2)) {
					hashMap.get(field2).add(newTuple);
				} else {
					ArrayList<Tuple> list = new ArrayList<Tuple>();
					list.add(newTuple);
					hashMap.put(field2, list);
				}
	        }
		} catch (DbException e) {
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			e.printStackTrace();
		}
        
    }

    public JoinPredicate getJoinPredicate() {
        return p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
    	return  child1.getTupleDesc().getFieldName(p.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        return child2.getTupleDesc().getFieldName(p.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        return TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        child1.open();
        child2.open();
    }

    public void close() {
        child1.close();
        child2.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child1.rewind();
        child2.rewind();
        tmpTuplesList.clear();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if (!tmpTuplesList.isEmpty()) {
    		return tmpTuplesList.remove();
    	} 
    	
    	while (child1.hasNext()) {
    		Tuple newTuple = child1.next();
    		Field field1 = newTuple.getField(p.getField1());
    		if (p.getOperator().equals(Predicate.Op.EQUALS)) {
    			if (hashMap.containsKey(field1)) {
        			Iterator<Tuple> itr = hashMap.get(field1).iterator();
        			while (itr.hasNext()) {
        				tmpTuplesList.add(joinTwoTuple(newTuple, itr.next()));
        			}
        			break;
        		}
    		} else {
    			for (Field f2: hashMap.keySet()) {
    				ArrayList<Tuple> list2 = hashMap.get(f2);
    				//boolean found = false;
    				for(Tuple t2: list2) {
    					if (p.filter(newTuple, t2)) {
    						tmpTuplesList.add(joinTwoTuple(newTuple, t2));
    					//	found = true;
    					}
    				}
    				
    			}
    		}
    		
    	}
    	
      if (!tmpTuplesList.isEmpty()) {
  		return tmpTuplesList.remove();
  	  } 
    
      return null;
    }
    
    /**
     * helper method used to merge two tuples
     * @param t1 tuple no 1
     * @param t2 tuple no 2
     * @return merged tuple
     */
    private Tuple joinTwoTuple(Tuple t1, Tuple t2) {
    	Tuple result = new Tuple(getTupleDesc());
    	
    	int index = 0;
    	for (int i = 0; i < t1.fieldsList.size(); i++) {
    		result.setField(index, t1.getField(i));
    		index++;
    	}
    	
    	for (int i = 0; i < t2.fieldsList.size(); i++) {
    		result.setField(index, t2.getField(i));
    		index++;
    	}
    	
    	return result;
    }

    @Override
    public DbIterator[] getChildren() {
//        if (child1.hashCode() <= child2.hashCode()) {
//        	return new DbIterator[] {child1, child2};
//        } else {
//        	return new DbIterator[] {child2, child1};
//        }
    	return new DbIterator[] {child1, child2};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        child1 = children[0];
        child2 = children[1];
    }

}
