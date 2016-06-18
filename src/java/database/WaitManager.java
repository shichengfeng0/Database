package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class WaitManager {
	Map<TransactionId, ArrayList<TransactionId>> waitMap; //<A, B> transaction A is waiting for B
	
	public WaitManager() {
		waitMap = new HashMap<TransactionId, ArrayList<TransactionId>>();
	}
	
	/**
	 * check whether transaction1 is waiting to transaction2
	 * @param tid1
	 * @param tid2
	 * @return true if transaction1 is waiting for transaction2
	 * 		   false otherwise
	 */
	public boolean checkT1WaitForT2(TransactionId tid1, TransactionId tid2) {
		return helperCheckT1WaitForT2(tid1, tid2, new ArrayList<TransactionId>());
//			if (!waitMap.containsKey(tid1) || tid1.equals(tid2)) {
//				return false;
//			} else {
//				ArrayList<TransactionId> waitList = waitMap.get(tid1);
//				
//				boolean contains = false;
//				for (int i = 0; waitList != null && i < waitList.size(); i++) {
//					TransactionId tmiddle = waitList.get(i);
//					if (tmiddle.equals(tid2)) {
//						return true;
//					} else {
//						if (checkT1WaitForT2(tmiddle, tid2)) {
//							return true;
//						}
//					}
//				}
//				return contains;
//			}
	}
	
	private boolean helperCheckT1WaitForT2(TransactionId tid1, TransactionId tid2, ArrayList<TransactionId> seenSoFar) {
		if (!waitMap.containsKey(tid1) || tid1.equals(tid2)) {
			return false;
		} else {
			ArrayList<TransactionId> waitList = waitMap.get(tid1);
			
			boolean contains = false;
			for (int i = 0; waitList != null && i < waitList.size(); i++) {
				TransactionId tmiddle = waitList.get(i);
				
				if (tmiddle.equals(tid2)) {
					return true;
				}
				
				if (seenSoFar.contains(tmiddle)) {
					return true;//there is another deadlock
				} else {
					seenSoFar.add(tmiddle);
				}
				
				if (helperCheckT1WaitForT2(tmiddle, tid2, seenSoFar)) {
					return true;
				}
			
			}
			return contains;
		}
	}
	
	/**
	 * transaction1 is waiting for transasction2
	 * @param tid
	 * @param tid2
	 */
	public synchronized void addWaitPair(TransactionId tid1, TransactionId tid2) {
		if (waitMap.containsKey(tid1)) {
			ArrayList<TransactionId> transactionIdList = waitMap.get(tid1);
			if (!transactionIdList.contains(tid2)) {
				transactionIdList.add(tid2);
			}
		} else {
			ArrayList<TransactionId> transactionIdList = new ArrayList<TransactionId>();
			transactionIdList.add(tid2);
			waitMap.put(tid1, transactionIdList);
		}
	}

	
	
	/**
	 * remove all wait pair with include the given transaction
	 * @param tid
	 */
	public synchronized void getRidTransaction(TransactionId tid) {
//		synchronized(this) {
			waitMap.remove(tid);
			ArrayList<TransactionId> waitForThatTransactionEmpty = new ArrayList<TransactionId>();
			
			
			for (TransactionId t: waitMap.keySet()) {
				ArrayList<TransactionId> waitList = waitMap.get(t);
				waitList.remove(tid);
				if (waitList.isEmpty()) {
					waitForThatTransactionEmpty.add(t);
				}
			}
			
			for (TransactionId t: waitForThatTransactionEmpty) {
				waitMap.remove(t);
			}
//		}
	}
	
	//only used for test
	public void printInfo() {
		System.out.println("==============================");
		for (TransactionId t: waitMap.keySet()) {
			String result = t.toString() + " wait for ";
			for (TransactionId t1: waitMap.get(t)) {
				result += " " + t1.toString(); 
			}
			
			
			System.out.println(result);
		}
		System.out.println("==============================");
	}
}
