package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    
    boolean called;
    TransactionId tid;
    DbIterator child;
    
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.child = child;
        tid = t;
        called = false;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (called) {
        	return null;
        }
        called = true;
        int num = 0;
        BufferPool bf = Database.getBufferPool();
        while (child.hasNext()) {
        	num++;
        	try {
				bf.deleteTuple(tid, child.next());
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
