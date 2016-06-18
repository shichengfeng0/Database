package simpledb;

import java.util.*;

public class LockManager {
	// the map used to keep track of all the lock information
	Map<PageId, ArrayList<Lock>> lockMap;
	WaitManager waitManager;
	
	
	/**
	 * construct the LockManager
	 */
	public LockManager() {
		lockMap = new HashMap<PageId, ArrayList<Lock>>();
		waitManager = new WaitManager();
	}
	
	/**
	 * 
	 * @param tid the transaction that require the lock
	 * @param pid the page id that the transaction need write lock
	 * @return true if we could get the write lock for this page
	 * 		   false otherwise
	 */
	public synchronized boolean requireWriteLock(TransactionId tid, PageId pid) {
		if (lockMap.containsKey(pid)) {
			ArrayList<Lock> lockList = lockMap.get(pid);
			
			if (lockList.size() == 1) {
				Lock onlyLock = lockList.get(0);
				if (onlyLock.getLockType() == Lock.WRITE_LOCK) {
					if (onlyLock.getTransactionId().equals(tid)) {
						return true;
					} else {
						waitManager.addWaitPair(tid, onlyLock.getTransactionId());
						
//						System.out.println("transaction: " + onlyLock.getTransactionId() + " is current holding write lock for pid =  " + pid.toString());
						
						return false;
					}
				} else {
					if (onlyLock.getTransactionId().equals(tid)) {
						upgradeLock(pid, tid);
						return true;
					} else {
						waitManager.addWaitPair(tid, onlyLock.getTransactionId());
						
//						System.out.println("transaction: " + onlyLock.getTransactionId() + " is current holding read lock for pid =  " + pid.toString());
						
						return false;
					}
				}
			} else {//multiple read locks
				
				String test = "";
				
				for (int i = 0; i < lockList.size(); i++) {
					Lock l = lockList.get(i);
					if (!l.getTransactionId().equals(tid)) {
						
//						System.out.println("add wait pair" + tid.toString() + " " + l.getTransactionId().toString());
						
						waitManager.addWaitPair(tid, l.getTransactionId());
						test += " " + l.getTransactionId();
					}
				}
				
//				System.out.println("my tid: " + tid.toString() + " wait other read lock transaction to release lock: " + test);
//				waitManager.printInfo();
//				printInfo();
				
				
				
				return false;
			}

		} else {// there is not any lock for this page
			ArrayList<Lock> lockList = new ArrayList<Lock>();
			lockList.add(new Lock(Lock.WRITE_LOCK, tid));
			lockMap.put(pid, lockList);
			return true;
		}
	}
	
	/**
	 * upgrade the read lock to the write lock
	 * @param pid
	 * @param tid
	 */
	public synchronized void upgradeLock(PageId pid, TransactionId tid) {
		ArrayList<Lock> lockList = new ArrayList<Lock>();
		lockList.add(new Lock(Lock.WRITE_LOCK, tid));
		lockMap.put(pid, lockList);
	}
	
	
	/**
	 * 
	 * @param tid the transaction id that need the read lock
	 * @param pid the page id that the transaction need the read lock
	 * @return true if the transaction could get the read lock
	 * 		   false otherwise
	 */
	public synchronized boolean requireReadLock(TransactionId tid, PageId pid) {
		if (lockMap.containsKey(pid)) {
			ArrayList<Lock> lockList = lockMap.get(pid);
			
			if (lockList.size() == 1) {
				Lock onlyLock = lockList.get(0);
				if (onlyLock.getLockType() == Lock.WRITE_LOCK) {
					if (onlyLock.getTransactionId().equals(tid)) {
						return true;
					} else {
						waitManager.addWaitPair(tid, onlyLock.getTransactionId());
						
//						System.out.println("transaction: " + onlyLock.getTransactionId() + " is current holding write lock for pid =  " + pid.toString());
						
						return false;
					}
				} else {
					if (!onlyLock.getTransactionId().equals(tid)) {
						lockList.add(new Lock(Lock.READ_LOCK, tid));
					}
					
					return true;
				}
			} else {//multiple read locks
				
//				String test = "";
				boolean alreadyHave = false;
				for (int i = 0; i < lockList.size(); i++) {
					Lock l = lockList.get(i);
					if (l.getTransactionId().equals(tid)) {
						alreadyHave = true;
					}
				}
				
				if (!alreadyHave) {
					lockList.add(new Lock(Lock.READ_LOCK, tid));
				}
				
				return true;
			}

		} else {
			ArrayList<Lock> lockList = new ArrayList<Lock>();
			lockList.add(new Lock(Lock.READ_LOCK, tid));
			lockMap.put(pid, lockList);
			return true;
		}
	}
	

	
	/**
	 * 
	 * @param pid the page id that we want to remove all locks for that page
	 * @return true if we remove the lock list successfully,
	 * 		   false otherwise
	 */
	public synchronized boolean removeLockForPage(PageId pid) {
		if (!lockMap.containsKey(pid)) {
			return false;
		}
		
		lockMap.remove(pid);
		return true;
	}
	
	/**
	 * This method will block the current process if lock manager can not get the write lock
	 * @param tid the transaction id of the transaction that needs the write lock 
	 * @param pid the page id of the page that transaction need to require write lock from
	 * @return true if finally we could get the write lock
	 */
	public boolean blockUntilGetWriteLock(TransactionId tid, PageId pid) {
			int time = 0;
			while (!requireWriteLock(tid, pid)) {
				time += 100;
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
//				System.out.println("tid: " + tid.toString());
//				System.out.println("pid: " + pid.toString());
			
				
//				System.out.println("get into while loop for blockUntilGetWriteLock");
				ArrayList<Lock> existLockList = lockMap.get(pid);
				
				synchronized(existLockList) {
					for (int i = 0; i < existLockList.size(); i++) {
						Lock l = existLockList.get(i);
						TransactionId tid2 = l.getTransactionId();

						if (waitManager.checkT1WaitForT2(tid2, tid)) {//dead lock happen, need to abort this transaction
//									System.out.println("tid2 wait for tid1 22222222222222222222222222");
							
//									System.out.println("detect deadlock");
							waitManager.getRidTransaction(tid);
//									System.out.println("11111111111111111");
							releaseAllLockForTransaction(tid);
							return false;
						}
					}
				}
				
				
				
				if (time > 1000 * 30) {
					return false;
				}
			}
			
//				System.out.println("get into return true of blockUntilGetWriteLock true");
			
			return true;
		
			

	}
	
	/**
	 *  This method will block the current process if lock manager can not get the read lock
	 * @param tid the transaction id of the transaction that needs the read lock 
	 * @param pid the page id of the page that transaction need to require read lock from
	 * @return true if eventually we could get the lock
	 * 		   false otherwise
	 */
	public boolean blockUntilGetReadLock(TransactionId tid, PageId pid) {
		int time = 0;	
		while (!requireReadLock(tid, pid)) {				
//				System.out.println("get into blockUntilGetReadLock");
			
				ArrayList<Lock> existLockList = lockMap.get(pid);
			synchronized(existLockList) {
				for (int i = 0; i > existLockList.size(); i++) {
					Lock l = existLockList.get(i);
					TransactionId tid2 = l.getTransactionId();
					if (waitManager.checkT1WaitForT2(tid2, tid)) {//dead lock happen, need to abort this transaction
//						System.out.println("detect deadlock");
						waitManager.getRidTransaction(tid);
						releaseAllLockForTransaction(tid);
						return false;
					} 
				}
				
			}
				
				
				try {
					time += 100;
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
				if(time > 30 * 1000) {
					return false;
				}
				
			}
			return true;
	}
	
	
	/**
	 * release all the locks corresponding to the given transaction id
	 * @param tid the given transaction id that all locks come from should be removed
	 */
	public synchronized void releaseAllLockForTransaction(TransactionId tid) {
		ArrayList<PageId> emptyLockPageId = new ArrayList<PageId>();
		for(PageId pid : lockMap.keySet()) {
			ArrayList<Lock> lockList = lockMap.get(pid);
			for (int i = 0; i < lockList.size(); i++) {
				Lock l = lockList.get(i);
				if (l.getTransactionId().equals(tid)) {
					lockList.remove(i);
					i--;
				}
			}
			
			if (lockList.isEmpty()) {
				emptyLockPageId.add(pid);
			}
		}
		
		// remove all pages with the no locks but still kept in the lock map
		for (PageId pid: emptyLockPageId) {
			lockMap.remove(pid);
		}
	}
	
	/**
	 * Releases the lock on a page
	 * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
	 */
	public synchronized boolean releasePage(PageId pid, TransactionId tid) {
		if (lockMap.containsKey(pid)) {
			ArrayList<Lock> lockList = lockMap.get(pid);
			for (int i = 0; i < lockList.size(); i++) {
				Lock l = lockList.get(i);
				if (l.getTransactionId().equals(tid)) {
					lockList.remove(i);
					break;
				}
			}
			
			if (lockList.isEmpty()) {
				lockMap.remove(pid);
			}
			
			return true;
		} else {
			return false;//no such page id
		}
	}
	
	/**
	 * release all locks for that page
	 * @param pid page id
	 */
	public synchronized boolean releasePage(PageId pid) {
		if (lockMap.containsKey(pid)) {
			lockMap.remove(pid);
			return true;
		} else {
			return false;
		}
	}
	
	/**
	 * block the process until the specified page has no locks
	 * @param pid
	 */
	public void blockUntilPageHasNoLock(PageId pid) {
		int totalTime = 0;
		while (hasLock(pid)) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			totalTime += 100;
			
//			if (totalTime > 1000) {
//				try {
//					throw new TransactionAbortedException();
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
//			}
		}
	}
	
	
	/**
	 * Return true if the specified transaction has a lock on the specified page
	 */
	public boolean holdsLock(PageId pid, TransactionId tid) {
		if (!lockMap.containsKey(pid)) {
			return false;
		} else {
			ArrayList<Lock> lockList = lockMap.get(pid);
			for (int i = 0; i < lockList.size(); i++) {
				if (lockList.get(i).getTransactionId().equals(tid)) {
					return true;
				}
			}
			return false;
		}
	}
	
	/**
	 * 
	 * @param pid the page id belongs to the page that we want to know whether it has lock or not
	 * @return true if the page has any lock
	 * 		   false otherwise
	 */
	public boolean hasLock(PageId pid) {
		return lockMap.containsKey(pid);
	}
	
	
	public WaitManager getWaitManager() {
		return waitManager;
	}
	
	//only used for testing
	public void printInfo() {
		System.out.println("============================================");
		for (PageId pid: lockMap.keySet()) {
			String result = pid.toString() + " has locks: ";
			for (Lock l : lockMap.get(pid)) {
				result += l.toString();
			}
			System.out.println(result);
		}
		
		System.out.println("============================================");
	}
}
