package _4_transaction;

public class LockAgent {
	
	
	public boolean acquireLock(int tid, int tableId, int pid, Permissions perm) {
		//Not yet complete,�״γ��� BufferPool.getPage()
		return true;
	}
	
	public void releasePage(int tid, int tableId, int pid) {
		//Not yet complete,�״γ��� BufferPool.releasePage()
	}
	
	public boolean holdsLock(int tid, int tableId, int pid) {
		//Not yet complete,�״γ��� BufferPool.holdsLock()
		return true;
	}

}