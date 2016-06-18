package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    
    boolean called;
    TransactionId tid;
    DbIterator child;
    int tableid;
    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
    	if (!Database.getCatalog().getDatabaseFile(tableid).getTupleDesc().equals(child.getTupleDesc())) {
    		throw new DbException("TupleDescs do not match");
    	}
    	
    	called = false;
        tid = t;
        this.child = child;
        this.tableid = tableid;
    }

    public TupleDesc getTupleDesc() {
    	Type[] typeArr = new Type[]{Type.INT_TYPE};
    	TupleDesc td = new TupleDesc(typeArr);
    	return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
    }

    public void close() {
        child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (called) {
        	return null;
        }
    	
        called = true;
        
    	int num = 0;
        BufferPool bf = Database.getBufferPool();
    	while(child.hasNext()) {
        	num++;
        	try {
				bf.insertTuple(tid, tableid, child.next());
			} catch (NoSuchElementException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
        }
    	
    	Type[] typeArr = new Type[]{Type.INT_TYPE};
    	TupleDesc td = new TupleDesc(typeArr);
    	Tuple tuple = new Tuple(td);
    	tuple.setField(0, new IntField(num));
    	return tuple;
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
