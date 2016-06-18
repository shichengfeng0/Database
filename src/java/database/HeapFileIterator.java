
package simpledb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;


public class HeapFileIterator implements DbFileIterator {
	TransactionId tid;
	ArrayList<Tuple> tupleArrayList; //all the tuples in that heap file
	ArrayList<HeapPage> heapPageArrayList;
	int curPointer;
	HeapFile hf;
	boolean open; //used to keep track of whether this iterator is open or not
	
	/**
	 * construct the HeapFileIterator
	 * @param hf HeapFile according to which we create this iterator
	 * @param tid TransactionId
	 */
	public HeapFileIterator(HeapFile f, TransactionId tid) {
		open = false;
		hf = f;
		curPointer = 0;
		this.tid = tid;
		heapPageArrayList = new ArrayList<HeapPage>();
		tupleArrayList = new ArrayList<Tuple>();
		
		BufferPool bf = Database.getBufferPool();

		for (int i = 0; i < f.numPages(); i++) {
			try {
				HeapPage newPage = (HeapPage) bf.getPage(tid, new HeapPageId(hf.getId(), i), Permissions.READ_ONLY);
				heapPageArrayList.add(newPage);
			} catch (TransactionAbortedException e) {
				e.printStackTrace();
			} catch (DbException e) {
				e.printStackTrace();
			}
		}
		
		for (HeapPage hp: heapPageArrayList) {
			Iterator<Tuple> itr = hp.iterator();
			while (itr.hasNext()) {
				tupleArrayList.add(itr.next());
			}
		}
	}
	
	@Override
	public void open() throws DbException, TransactionAbortedException {
		open = true;
	}

	@Override
	public boolean hasNext() throws DbException, TransactionAbortedException {
		if (open == false) {
			return false;
		}
		
		return curPointer < tupleArrayList.size();
	}

	@Override
	public Tuple next() throws DbException, TransactionAbortedException,
			NoSuchElementException {
		if (open == false) {
			throw new NoSuchElementException("iterator is not open yet");
		}
		
		if (!hasNext()) {
			throw new NoSuchElementException("no more elements");
		}
		
		curPointer++;
		return tupleArrayList.get(curPointer - 1);
	}

	@Override
	public void rewind() throws DbException, TransactionAbortedException {
		if (open == false) {
			throw new IllegalStateException("iterator is not open yet");
		}
		
		curPointer = 0;
	}

	@Override
	public void close() {
		open = false;
	}

}


