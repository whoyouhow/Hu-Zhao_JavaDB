package _1_basic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;

import _4_transaction.Permissions;

/**
 * A heap file stores a collection of tuples. It is also responsible for managing pages.
 * It needs to be able to manage page creation as well as correctly manipulating pages
 * when tuples are added or deleted.
 * @author Sam Madden modified by Doug Shook
 *
 */
public class HeapFile {
	
	public static final int PAGE_SIZE = 4096;
	
	/**
	 * Creates a new heap file in the given location that can accept tuples of the given type
	 * @param f location of the heap file
	 * @param types type of tuples contained in the file
	 */
	private TupleDesc desc;
	private File file;
	public HeapFile(File f, TupleDesc type) {
		//your code here
		file = f;
		desc = type;
	}
	
	public File getFile() {
		//your code here
		return this.file;
	}
	
	public TupleDesc getTupleDesc() {
		//your code here
		return this.desc;
	}
	
	/**
	 * Creates a HeapPage object representing the page at the given page number.
	 * Because it will be necessary to arbitrarily move around the file, a RandomAccessFile object
	 * should be used here.
	 * @param id the page number to be retrieved
	 * @return a HeapPage at the given page number
	 * @throws IOException 
	 */
	public HeapPage readPage(int id){
		//your code here
		byte[] content = new byte[PAGE_SIZE];
		HeapPage hp = null;
		
		RandomAccessFile file;
		try {
			file = new RandomAccessFile(this.getFile(), "r");
			file.seek(PAGE_SIZE * id);
			file.readFully(content, 0, PAGE_SIZE);
			file.close();
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e2){
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		
		try {
			hp = new HeapPage(id, content, this.getId());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return hp;
	}
	
	/**
	 * Returns a unique id number for this heap file. Consider using
	 * the hash of the File itself.
	 * @return
	 */
	public int getId() {
		//your code here
		return this.file.hashCode();
	}
	
	/**
	 * Writes the given HeapPage to disk. Because of the need to seek through the file,
	 * a RandomAccessFile object should be used in this method.
	 * @param p the page to write to disk
	 */
	public void writePage(HeapPage p) {
		//your code here
		try {
			RandomAccessFile file = new RandomAccessFile(this.getFile(), "rw");
			file.seek(PAGE_SIZE * p.getId());
			file.write(p.getPageData());
			file.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Adds a tuple. This method must first find a page with an open slot, creating a new page
	 * if all others are full. It then passes the tuple to this page to be stored. It then writes
	 * the page to disk (see writePage)
	 * @param t The tuple to be stored
	 * @return The HeapPage that contains the tuple
	 */
	public HeapPage addTuple(Tuple t) {
		//your code here
		boolean found = false;
		HeapPage hp = null;
		for (int i = 0; i < this.getNumPages(); i ++){
			hp = this.readPage(i);
			
			for (int j = 0; j < hp.getNumSlots(); j ++){
				if (!hp.slotOccupied(j)){
					found = true;
//					this.writePage(hp);
//					return hp;
					break;
				}
			}
			if (found){
				
				try {
					hp.addTuple(t);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}	
				this.writePage(hp);
				
				break;
			}
		}
		return hp;
		
	}
	
	/**
	 * This method will examine the tuple to find out where it is stored, then delete it
	 * from the proper HeapPage. It then writes the modified page to disk.
	 * @param t the Tuple to be deleted
	 */
	public void deleteTuple(Tuple t){
		//your code here
		HeapPage hp = this.readPage(t.getPid());
		hp.deleteTuple(t);
		this.writePage(hp);
	}
	
	/**
	 * Returns an ArrayList containing all of the tuples in this HeapFile. It must
	 * access each HeapPage to do this (see iterator() in HeapPage)
	 * @return
	 */
	public ArrayList<Tuple> getAllTuples() {
		//your code here
		ArrayList<Tuple> allTuples = new ArrayList<Tuple>();
		for (int i = 0; i < this.getNumPages(); i ++){
			HeapPage hp = readPage(i);
			Iterator<Tuple> hpIterator = hp.iterator();
			while (hpIterator.hasNext()){
//				Tuple tuple = hp.iterator().next();
				allTuples.add(hpIterator.next());
				
			}
		}
		return allTuples;
	}
	
	/**
	 * Computes and returns the total number of pages contained in this HeapFile
	 * @return the number of pages
	 */
	public int getNumPages() {
		//your code here
		return (int) Math.ceil((double)this.getFile().length() / PAGE_SIZE);
	}
	
	public void addLock(Integer pid, int tid, Permissions permission) {
		HeapPage hp = this.readPage(pid);
		hp.setLock(permission);
		hp.setLocked(true);
		hp.setLocker(tid);
		this.writePage(hp);
	}
}
