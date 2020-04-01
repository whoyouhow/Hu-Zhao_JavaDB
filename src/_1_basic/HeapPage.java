package _1_basic;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.imageio.event.IIOReadProgressListener;

import _4_transaction.Permissions;

public class HeapPage {

	private int id;
	public byte[] header;
	private Tuple[] tuples;
	private TupleDesc td;
	private int numSlots;
	private int tableId;
	
	//$$$$$$$$HW4 addition
	private boolean dirty;
	private Permissions lock;
	private boolean isLocked;
	private int locker;
	//$$$$$$$$$$



	public HeapPage(int id, byte[] data, int tableId) throws IOException {
		this.id = id;
		this.tableId = tableId;
		
		//$$$$$$$$HW4 addition
		this.dirty = false;
		this.isLocked = false;
		//$$$$$$$$$$

		this.td = Database.getCatalog().getTupleDesc(this.tableId);
		this.numSlots = getNumSlots();
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

		// allocate and read the header slots of this page
		header = new byte[getHeaderSize()];
		for (int i=0; i<header.length; i++)
			header[i] = dis.readByte();

		try{
			// allocate and read the actual records of this page
			tuples = new Tuple[numSlots];
			for (int i=0; i<tuples.length; i++)
				tuples[i] = readNextTuple(dis,i);
		}catch(NoSuchElementException e){
			e.printStackTrace();
		}
		dis.close();
	}

	public int getId() {
		//your code here
		return this.id;
	}
	
	public int getTableId() {
		return this.tableId;
	}

	/**
	 * Computes and returns the total number of slots that are on this page (occupied or not).
	 * Must take the header into account!
	 * @return number of slots on this page
	 */
	public int getNumSlots() {
		//your code here
		int numSlots = (int) ((double)HeapFile.PAGE_SIZE * 8 / (td.getSize() * 8 + 1));
		//System.out.printf("numSlots %d \n", numSlots);
		return numSlots;
	}

	/**
	 * Computes the size of the header. Headers must be a whole number of bytes (no partial bytes)
	 * @return size of header in bytes
	 */
	private int getHeaderSize() {        
		//your code here
		return (int) Math.ceil((double)this.getNumSlots() / 8);
	}

	/**
	 * Checks to see if a slot is occupied or not by checking the header
	 * @param s the slot to test
	 * @return true if occupied
	 */
	public boolean slotOccupied(int s) {
		//your code here
		if((byte) this.header[s / 8] == -1) { //11111111, not applicable in the calculation below because (-1)%2==-1 in java
			return true;
		}
		else {
			return ((byte) this.header[s / 8] >> (s % 8)) % 2 == 1;
		}
		
		
	}

	/**
	 * Sets the occupied status of a slot by modifying the header
	 * @param s the slot to modify
	 * @param value its occupied status
	 */
	public void setSlotOccupied(int s, boolean value) {
		//your code here
		
		if(value == true) {
			header[s / 8] = (byte) (header[s / 8] | (1 << s % 8)); //e.g. 00011111 | 00100000 -> 00111111
		}
		else {
			header[s / 8] = (byte) (header[s / 8] & ~(1 << s % 8)); //e.g. 00111111 & 11011111 -> 00011111
		}
	}
	
	/**
	 * Adds the given tuple in the next available slot. Throws an exception if no empty slots are available.
	 * Also throws an exception if the given tuple does not have the same structure as the tuples within the page.
	 * @param t the tuple to be added.
	 * @throws Exception
	 */
	public void addTuple(Tuple t) throws Exception {
		//your code here
		boolean addSuccessful = false;
		if(t.getDesc().equals(this.td) == false) {
			throw new Exception("Tuple schema not match!");
		}
		for (int i=0; i<this.getNumSlots(); i++) { //find the next empty slot
			//System.out.printf("slot=%d, numSlots=%d, occupied=%b\n", i,this.getNumSlots(),slotOccupied(i));
			if (this.slotOccupied(i) == false) {
				this.setSlotOccupied(i, true);
				this.tuples[i] = t;
				addSuccessful = true;
				break;
			}
			
		}
		if (addSuccessful == false) {
			throw new Exception("No empty slot!");
		}
	}

	/**
	 * Removes the given Tuple from the page. If the page id from the tuple does not match this page, throw
	 * an exception. If the tuple slot is already empty, throw an exception
	 * @param t the tuple to be deleted
	 * @throws Exception
	 */
	public void deleteTuple(Tuple t) {
		//your code here
		int slotId = t.getId();
		setSlotOccupied(slotId, false);
		tuples[slotId] = null;
	}
	
	/**
     * Suck up tuples from the source file.
     */
	private Tuple readNextTuple(DataInputStream dis, int slotId) {
		// if associated bit is not set, read forward to the next tuple, and
		// return null.
		if (!slotOccupied(slotId)) {
			for (int i=0; i<td.getSize(); i++) {
				try {
					dis.readByte();
				} catch (IOException e) {
					throw new NoSuchElementException("error reading empty tuple");
				}
			}
			return null;
		}

		// read fields in the tuple
		Tuple t = new Tuple(td);
		t.setPid(this.id);
		t.setId(slotId);

		for (int j=0; j<td.numFields(); j++) {
			if(td.getType(j) == Type.INT) {
				byte[] field = new byte[4];
				try {
					dis.read(field);
					t.setField(j, new IntField(field));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				byte[] field = new byte[129];
				try {
					dis.read(field);
					t.setField(j, new StringField(field));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}


		return t;
	}

	/**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
	 *
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @return A byte array correspond to the bytes of this page.
     */
	public byte[] getPageData() {
		int len = HeapFile.PAGE_SIZE;
		ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
		DataOutputStream dos = new DataOutputStream(baos);

		// create the header of the page
		for (int i=0; i<header.length; i++) {
			try {
				dos.writeByte(header[i]);
			} catch (IOException e) {
				// this really shouldn't happen
				e.printStackTrace();
			}
		}

		// create the tuples
		for (int i=0; i<tuples.length; i++) {

			// empty slot
			if (!slotOccupied(i)) {
				for (int j=0; j<td.getSize(); j++) {
					try {
						dos.writeByte(0);
					} catch (IOException e) {
						e.printStackTrace();
					}

				}
				continue;
			}

			// non-empty slot
			for (int j=0; j<td.numFields(); j++) {
				Field f = tuples[i].getField(j);
				try {
					dos.write(f.toByteArray());

				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		// padding
		int zerolen = HeapFile.PAGE_SIZE - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
		byte[] zeroes = new byte[zerolen];
		try {
			dos.write(zeroes, 0, zerolen);
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			dos.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return baos.toByteArray();
	}

	/**
	 * Returns an iterator that can be used to access all tuples on this page. 
	 * @return
	 */
	public Iterator<Tuple> iterator() {
		//your code here
//		return Arrays.asList(this.tuples).iterator(); //has problem: the length of the iterable list is the lenth of whole array, instead of occupied
		ArrayList<Tuple> tuplelist = new ArrayList<Tuple>();
		for (int i = 0; i < this.tuples.length; i++)
			if(this.slotOccupied(i)){
				tuplelist.add(this.tuples[i]);
			}
		return tuplelist.iterator();
	}
	
	//$$$$$$$$HW4 addition
	public void setDirty(boolean value) {
		this.dirty = value;
	}
	
	public boolean isDirty() {
		return this.dirty;
	}
	
	public Permissions getLock() {
		return this.lock;
	}
	
	public int getLocker() {
		return this.locker;
	}
	
	public boolean isLocked() {
		return this.isLocked;
	}
	
	public void setLock(Permissions perm) {
		this.lock = perm;
	}
	
	public void setLocker(int tid) {
		this.locker = tid;
	}
	
	public void setLocked(boolean locked) {
		this.isLocked = locked;
	}
	
	public void addLock(int tid, Permissions perm) {
		this.setLock(perm);
		this.setLocked(true);
		this.setLocker(tid);
	}
	
	public void releaseLock() {
		this.setLock(null);
		this.setLocker(0);
		this.setLocked(false);
	}
	//$$$$$$$$$$
}
