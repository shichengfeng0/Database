package simpledb;

import java.io.*;

import java.util.ArrayList;
import java.util.Random;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    
    int maxPages;
    //pageArrayList 
    
    ArrayList<Page> pageArrayList; 
    
    LockManager lockManager;
    
    
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        maxPages = numPages;
//        System.out.println("inilized the bf with max page " + maxPages);
        
        pageArrayList = new ArrayList<Page>();
        lockManager = new LockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
		if (perm.toString().equals("READ_WRITE")) {
			boolean getLock =lockManager.blockUntilGetWriteLock(tid, pid);
        	if (!getLock) {
        		try {
					transactionComplete(tid, false);
				} catch (IOException e) {
					e.printStackTrace();
				}
        		throw new TransactionAbortedException();
        	}
		
        } else {
        	boolean getLock = lockManager.blockUntilGetReadLock(tid, pid);
        	if (!getLock) {
        		try {
					transactionComplete(tid, false);
				} catch (IOException e) {
					e.printStackTrace();
				}
        		throw new TransactionAbortedException();
        	}
        }
        
		synchronized(this) {
			int pageIndex = findPageIndex(pid);
	        if (pageIndex != -1) {//if the page is already in the BufferPool
	        	return pageArrayList.get(pageIndex);
	        } else {//if the the page is not in the BufferPool
	        	if (pageArrayList.size() >= maxPages) {
	        		evictPage();
	        	}
	        	
	        	//goes in to heap file to fetch pages
	        	int tableid = pid.getTableId();
	        	Catalog catalog = Database.getCatalog();
	        	
	        	Page newPage = catalog.getDatabaseFile(tableid).readPage(pid);
	        	pageArrayList.add(newPage);
	        	
	        	return newPage;
	        }
		}
        
        
    	
    }
    
//    private synchronized void removeAllDirtyPageForTransaction(TransactionId tid) {
//		for (int i = 0; i < pageArrayList.size(); i++) {
//			Page p = pageArrayList.get(i);
//			TransactionId dirtyTid = p.isDirty();
//			if (dirtyTid != null) {
//				if (dirtyTid.equals(tid)) {
////					System.out.println("remove the aborted tid page");
//					pageArrayList.remove(i);
//					i--;
//				}
//			}
//		}
//	}

	/**
     * helper method that is used to find the specific page according to the page id
     * @param pid according to which we try to find the page
     * @return page index in the pageArrayList
     * 		   -1 if we can not find the page
     */
    public int findPageIndex(PageId pid) {
    	synchronized(pageArrayList) {
    		for (int i = 0; i < pageArrayList.size(); i++) {
        		if (pageArrayList.get(i).getId().equals(pid)) return i;
        	}
        	return -1;
    	}
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public synchronized void releasePage(TransactionId tid, PageId pid) {
        lockManager.releasePage(pid, tid);
        WaitManager waitManager = lockManager.getWaitManager();
        waitManager.getRidTransaction(tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public synchronized void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        return lockManager.holdsLock(pid, tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public synchronized void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
    	
    	if (commit) {
    		for (int i = 0; i < pageArrayList.size(); i++) {
    			Page p = pageArrayList.get(i);
    			TransactionId dirtier = p.isDirty();
    			if (dirtier != null && dirtier.equals(tid)) {
    				flushPage(p.getId());
    			}
    		}
        } else {
//        	System.out.println("transaction aborted");
        	// abort situation
        	discardAllPagesWithDirtyTranscation(tid);
        }
        
        // release all locks with related locks
    	lockManager.releaseAllLockForTransaction(tid);
    	WaitManager wm = lockManager.getWaitManager();
        wm.getRidTransaction(tid);
        
    }
    
    public synchronized void discardAllPagesWithDirtyTranscation(TransactionId tid) {
    	for (int i = 0; i < pageArrayList.size(); i++) {
    		Page p = pageArrayList.get(i);
    		TransactionId curTid = p.isDirty();
    		
    		if (curTid != null && curTid.equals(tid)) {
    			discardPage(p.getId());
    		}
    	}
    }
    
    
    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile hp = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> modifiedPages = hp.insertTuple(tid, t);
       
        for (int i = 0; i < modifiedPages.size(); i++) {
        	modifiedPages.get(i).markDirty(true, tid);
        }
        
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
    	DbFile hp = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> modifiedPages = hp.deleteTuple(tid, t);
        
        
        for (int i = 0; i < modifiedPages.size(); i++) {
        	modifiedPages.get(i).markDirty(true, tid);
        }
  
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
    	for (int i = 0; i < pageArrayList.size(); i++) {
    		flushPage(pageArrayList.get(i).getId());
    	}

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public void discardPage(PageId pid) {
        for (int i = 0; i < pageArrayList.size(); i++) {
        	Page p = pageArrayList.get(i);
        	if (p.getId().equals(pid)) {
        		DbFile dbfile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        		Page page = dbfile.readPage(pid);
        		pageArrayList.set(i, page);
        		break;
        	}
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private void flushPage(PageId pid) throws IOException {
    	for (int i = 0; i < pageArrayList.size(); i++) {
    		if (pageArrayList.get(i).getId().equals(pid)) {
    			Page p = pageArrayList.get(i);
    			
    			// append an update record to the log, with 
    	        // a before-image and after-image.
    	        TransactionId dirtier = p.isDirty();
    	        if (dirtier != null) {
    	        	
    	        	
    	        	Database.getLogFile().logWrite(dirtier, p.getBeforeImage(), p);
    	        	
    	        	//for test
    	        	@SuppressWarnings("unused")
					Page beforeImage = p.getBeforeImage();
    	        	
    	        	Database.getLogFile().force();
    	          
    	        	Catalog catalog = Database.getCatalog();
    	        	catalog.getDatabaseFile(pid.getTableId()).writePage(p);
  				
    	        	p.markDirty(false, null);// mark it as undirty
    	        	p.setBeforeImage();
    	        }
    		}
    	}
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        for (int i = 0; i < pageArrayList.size(); i++) {
        	Page p = pageArrayList.get(i);
        	
        	
        	TransactionId curTid = p.isDirty();
//        	System.out.println("curTid is null: " + curTid == null);
        	if (curTid != null && curTid.equals(tid)) {
        		flushPage(p.getId());
        	}
        	
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     * @throws DbException 
     */
    private synchronized void evictPage() {
    
    	// only evict clean pages
    	ArrayList<Integer> cleanPageIndex = new ArrayList<Integer>();
    	
    	for (int i = 0; i < pageArrayList.size(); i++) {
    		Page p = pageArrayList.get(i);
    		if (p.isDirty() == null) {
    			cleanPageIndex.add(i);
    		}
    	}
    	
    	
    	try {
    		if (cleanPageIndex.isEmpty()) {
    			throw new DbException("all the pages in the buffer pool are dirty");
        	}
    		Random r = new Random();
    		int evictIndex1 = r.nextInt(cleanPageIndex.size());
    	    int evictIndex2 = cleanPageIndex.get(evictIndex1);
    	    	
    	    	
			//flushPage(pid); no need since the page is clean
	    	synchronized(pageArrayList) {
	    		pageArrayList.remove(evictIndex2);
	    	}
			
			
		} catch (DbException e) {
			e.printStackTrace();
		}
    }
    
    /**
     * get the lock manager
     */
    public LockManager getLockManager() {
    	return lockManager;
    }
}
