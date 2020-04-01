package _4_transaction;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import _1_basic.Catalog;
import _1_basic.Database;
import _1_basic.HeapFile;
import _1_basic.HeapPage;
import _1_basic.Tuple;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool which check that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
//	public class Page{
//		private int hf;
//		private int hp;
//		public Page(int hf, int hp) {
//			this.hf = hf;
//			this.hp = hp;
//		}
//	}
	public int maxNumPages;
	public HashMap<ArrayList<Integer>, HeapPage> hpMap = new HashMap<ArrayList<Integer>, HeapPage>();
	private HashMap<Integer, ArrayList<HeapPage>> tcMap = new HashMap<Integer, ArrayList<HeapPage>>();
	private int deadLockRd = -1;
//	private Catalog c;
//	private HeapFile hf;
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // your code here
    	this.maxNumPages = DEFAULT_PAGES;
    	this.maxNumPages = numPages;
    	
    }
    
    public void print(Object b) {
		System.out.println(b);
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
     * @param tableId the ID of the table with the requested page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public HeapPage getPage(int tid, int tableId, int pid, Permissions perm)
        throws Exception {
        // your code here
    	int size = hpMap.size();
    	
    	if (size == maxNumPages) {
    		evictPage();
    		size = hpMap.size();
    		if (size == maxNumPages)
    			throw new Exception("Page out of index");
    	}
    	Catalog c = Database.getCatalog();
		HeapFile hf = c.getDbFile(tableId);
		HeapPage hp;
		ArrayList<Integer> page = new ArrayList<Integer>();
		page.add(hf.getId());
		page.add(pid);

		int rd = 2;
		if(this.deadLockRd == -1) {
			this.deadLockRd = 2;
		}else if(this.deadLockRd > 10){
			this.deadLockRd = 2;
			rd = this.deadLockRd + 2;
		}else {
			rd = this.deadLockRd + 2;
		}
		int i = 0;
		while(i < rd) {
			i++;
			try {
				TimeUnit.MILLISECONDS.sleep(5);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (hpMap.containsKey(page))
				hp = hpMap.get(page);
			else 
				hp = hf.readPage(pid);
	
			if (!hp.isLocked()) {
				hp.addLock(tid, perm);
	//			hf.writePage(pid);
	//			print(hf.readPage(pid).islocked());
				int hfid = hf.getId();
	
				ArrayList<Integer> l = new ArrayList<Integer>();
				l.add(hfid);
				l.add(pid);
				hpMap.put(l, hp);	
				
				this.uptc(tid, hp);
				return hp;
			}else if(hp.getLock()==Permissions.READ_ONLY) { //if locked, the lock is READ_ONLY
				if (perm==Permissions.READ_ONLY) {
					this.uptc(tid, hp);
					return hp;
				}
				else if (perm==Permissions.READ_WRITE && tid==hp.getLocker()) {
					hp.addLock(tid, perm);
					int hfid = hf.getId();
					ArrayList<Integer> l = new ArrayList<Integer>();
					l.add(hfid);
					l.add(pid);
					hpMap.put(l, hp);	
					this.uptc(tid, hp);
					return hp;
				}
			}else if(hp.getLock()==Permissions.READ_WRITE) { //if locked, the lock is READ_WRITE
				if (tid == hp.getLocker()) {
					hp.addLock(tid, perm);
					int hfid = hf.getId();
					ArrayList<Integer> l = new ArrayList<Integer>();
					l.add(hfid);
					l.add(pid);
					hpMap.put(l, hp);	
					this.uptc(tid, hp);
					return hp;
				}
//				return null;
			}
		}
		this.rollBack(tid);
        return null;
    }
    
    public void uptc(int tid, HeapPage hp) {
    	ArrayList<HeapPage> updatedAL = new ArrayList<HeapPage>();
		if(this.tcMap.get(tid) != null) {
			updatedAL = this.tcMap.get(tid);
		}
		updatedAL.add(hp);
		this.tcMap.put(tid, updatedAL);
    }
    
    public void rollBack(int tid) {
    	ArrayList<HeapPage> hps = this.tcMap.get(tid);
    	Catalog c = Database.getCatalog();
    	HeapFile hf;
    	for(int i = 0; i < hps.size(); i++) {
    		int pid = hps.get(i).getId();
//    		this.releasePage(tid, hps.get(i).getTableId(), pid);
    		ArrayList<Integer> l = new ArrayList<Integer>();
    		hf = c.getDbFile(hps.get(i).getTableId());
    		l.add(hf.getId());
    		l.add(pid);
    		HeapPage newhp =  hf.readPage(pid);
    		this.hpMap.put(l, newhp);
    	}
    	this.tcMap.remove(tid);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param tableID the ID of the table containing the page to unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(int tid, int tableId, int pid) {
        // your code here
    	Catalog c = Database.getCatalog();
		HeapFile hf = c.getDbFile(tableId);
		int hfid = hf.getId();
		ArrayList<Integer> l = new ArrayList<Integer>();
		l.add(hfid);
		l.add(pid);
		HeapPage hp = hpMap.get(l);
		hp.releaseLock();
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public   boolean holdsLock(int tid, int tableId, int pid) {
        // your code here
    	Catalog c = Database.getCatalog();
		HeapFile hf = c.getDbFile(tableId);
		int hfid = hf.getId();

		ArrayList<Integer> l = new ArrayList<Integer>();
		l.add(hfid);
		l.add(pid);
		HeapPage hp = hpMap.get(l);

		if (hp.isLocked() && hp.getLocker()==tid) {
			return true;
		}else {
			return false;
		}
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction. If the transaction wishes to commit, write
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public   void transactionComplete(int tid, boolean commit)
        throws IOException {
        // your code here
    	if (commit) {
    		Iterator iterator = hpMap.keySet().iterator();
        	while (iterator.hasNext()){
        		ArrayList<Integer> l = (ArrayList<Integer>) iterator.next();
        		HeapPage hp = hpMap.get(l);
        		int tableId = hp.getTableId();
        		int pid = hp.getId();
        		releasePage(tid, tableId, pid);
        		flushPage(tableId, pid);
        	}
    	}else {
    		Iterator iterator = hpMap.keySet().iterator();
        	while (iterator.hasNext()){
        		ArrayList<Integer> l = (ArrayList<Integer>) iterator.next();
        		HeapPage hp = hpMap.get(l);
        		int tableId = hp.getTableId();
        		int pid = hp.getId();
        		
        		hpMap.remove(l, hp);
        	}
		}
    	this.tcMap.remove(tid);
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to. May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public  void insertTuple(int tid, int tableId, Tuple t)
        throws Exception {
        // your code here
//    	HeapPage hp = getPage(tid, tableId, t.getPid(), Permissions.READ_WRITE);
    	Catalog c = Database.getCatalog();
    	HeapFile hf = c.getDbFile(tableId);
    	ArrayList<Integer> l = new ArrayList<Integer>();
    	l.add(hf.getId());
    	l.add(t.getPid());
    	HeapPage hp = hpMap.get(l);
    	if (hp!=null) {
    		if (hp.getLock() == Permissions.READ_ONLY) {
    			throw new Exception("No permission.");
    		}
    		hp.addTuple(t);
    		hp.setDirty(true);
    	}
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty.
     *
     * @param tid the transaction adding the tuple.
     * @param tableId the ID of the table that contains the tuple to be deleted
     * @param t the tuple to add
     */
    public  void deleteTuple(int tid, int tableId, Tuple t)
        throws Exception {
        // your code here
//    	HeapPage hp = getPage(tid, tableId, t.getPid(), Permissions.READ_WRITE);
    	Catalog c = Database.getCatalog();
    	HeapFile hf = c.getDbFile(tableId);
    	ArrayList<Integer> l = new ArrayList<Integer>();
    	l.add(hf.getId());
    	l.add(t.getPid());
    	HeapPage hp = hpMap.get(l);
    	if (hp!=null) {
    		if (hp.getLock() == Permissions.READ_ONLY) {
    			throw new Exception("No permission.");
    		}
    		hp.deleteTuple(t);
    		hp.setDirty(true);
    	}
    }

    private synchronized  void flushPage(int tableId, int pid) throws IOException {
        // your code here
    	Catalog c = Database.getCatalog();
    	HeapFile hf = c.getDbFile(tableId);
    	ArrayList<Integer> l = new ArrayList<Integer>();
    	l.add(hf.getId());
    	l.add(pid);
    	HeapPage hp = hpMap.get(l);
    	hf.writePage(hp);
    	hpMap.remove(l,hp);
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws Exception {
        // your code here
    	Iterator iterator = hpMap.keySet().iterator();
        while (iterator.hasNext()){
//            String key = (String)iterator.next();
//            System.out.println(key+"="+hashMap.get(key));
        	ArrayList<Integer> l = (ArrayList<Integer>) iterator.next();
        	HeapPage hp = hpMap.get(l);
        	if (!hp.isDirty()) {
        		hpMap.remove(l, hp);
        		break;
        	}
        }
    }

}
