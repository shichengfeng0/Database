package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	File backFile;
	TupleDesc tupleDesc;
	
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     * @throws FileNotFoundException 
     */
    public HeapFile(File f, TupleDesc td) {
        backFile = f;
        tupleDesc = td;
//        Database.getCatalog().addTable(this);
    } 

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return backFile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return backFile.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }
    
    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
    	if (pid.getTableId() != getId()) {
    		throw new IllegalArgumentException("page does not exist in this file");
    	}
    	
    	int offset = pid.pageNumber();
    	byte[] constructorArray = new byte[BufferPool.PAGE_SIZE];
    	
    	
    	try {
    		@SuppressWarnings("resource")
			FileInputStream fileInputStream = new FileInputStream(backFile);
			fileInputStream.skip(offset * BufferPool.PAGE_SIZE);
			fileInputStream.read(constructorArray, 0, BufferPool.PAGE_SIZE);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
    	
    	
    	HeapPageId newPid = (HeapPageId) pid;

        try {
			return new HeapPage(newPid, constructorArray);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
    }
    
    // see DbFile.java for javadocs
    public synchronized void writePage(Page page) throws IOException {
        HeapPage hp = (HeapPage) page;
        byte[] pageData = hp.getPageData();
        RandomAccessFile raf = new RandomAccessFile(backFile,"rw");
        long offset = hp.getId().pageNumber * BufferPool.PAGE_SIZE;
        raf.seek(offset);
        raf.write(pageData);
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int)backFile.length() / BufferPool.PAGE_SIZE;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
    	if (!t.getTupleDesc().equals(tupleDesc)) {
    		throw new DbException("tupleDesc does not match");
    	}
    	
    	ArrayList<Page> modifiedPages = new ArrayList<Page>();
    	BufferPool bf = Database.getBufferPool();
    	LockManager lockManager = bf.getLockManager();
    	for (int i = 0; i < numPages(); i++) {
    		HeapPageId pageId = new HeapPageId(getId(), i);
    		boolean alreadyHoldsLock = false;
    		if (lockManager.holdsLock(pageId, tid)) {
    			alreadyHoldsLock = true;
    		}
    		HeapPage page = (HeapPage) bf.getPage(tid, pageId, Permissions.READ_WRITE);
    		if (page.getNumEmptySlots() > 0) {
    			//if find empty slot in this page
    			page.insertTuple(t);
    			page.markDirty(true, tid);
    			modifiedPages.add(page);
    			//we do not write the page into the file until this page is evicted from the Buffer Pool
    			return modifiedPages;
    		} else {
    			if (!alreadyHoldsLock) {
    				lockManager.releasePage(pageId, tid);
    			}
    		}
    	}
    	
    	
    	//if we do not find any page containing empty slots, we have to create new pages in the heap file
    	byte[] newHeapPageData = HeapPage.createEmptyPageData();
    	
    	synchronized(this) {//prevent the situation that two transactions want to add new page at the same time
    		HeapPageId heapPageId = new HeapPageId(getId(), numPages());
        	HeapPage newHeapPage = new HeapPage(heapPageId, newHeapPageData);
        	writePage(newHeapPage);
        	
        	HeapPage hp = (HeapPage) bf.getPage(tid, heapPageId, Permissions.READ_WRITE);// not write the modified page into file until it is evicted from the Buffer Pool 
        	t.getRecordId().pageId = heapPageId; //update the pageid
        	hp.insertTuple(t);
        	hp.markDirty(true, tid);
        	modifiedPages.add(hp);
        	
            return modifiedPages;
    	}
    	
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
    	if (t.getRecordId().getPageId().getTableId() != getId()) {
        	throw new DbException("this tuple is not in this table (HeapFile)");
        }
    	
    	if (!t.getTupleDesc().equals(tupleDesc)) {
    		throw new DbException("tupleDesc does not match");
    	}
    	
    	ArrayList<Page> modifiedPages = new ArrayList<Page>();
    	
    	BufferPool bf = Database.getBufferPool();
    	RecordId rid = t.getRecordId();
    	PageId pid = rid.getPageId();
    	HeapPage hp = (HeapPage) bf.getPage(tid, pid, Permissions.READ_WRITE);
    	
    	hp.markDirty(true, tid);
    	
    	hp.deleteTuple(t);
    	modifiedPages.add(hp);
    	
        return modifiedPages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }
}

