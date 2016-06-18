package simpledb;

public class Lock {
	public static final int READ_LOCK = 0;
	public static final int WRITE_LOCK = 1;
	
	TransactionId tid;
	int lockType;
	
	public Lock(int lockType, TransactionId tid) {
		this.lockType = lockType;
		this.tid = tid;
	}
	
	public TransactionId getTransactionId() {
		return tid;
	}
	
	public int getLockType() {
		return lockType;
	}
	
	public void reset(int lockType) {
		this.lockType = lockType;
	}
	
	public String toString() {
		return "lock type: " + lockType + " " + "tid: " + tid.toString();
	}
}
