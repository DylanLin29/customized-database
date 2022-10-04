package hw1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * A heap file stores a collection of tuples. It is also responsible for
 * managing pages.
 * It needs to be able to manage page creation as well as correctly manipulating
 * pages
 * when tuples are added or deleted.
 * 
 * @author Sam Madden modified by Doug Shook
 *
 */
public class HeapFile {

	public static final int PAGE_SIZE = 4096;
	private TupleDesc td;
	private File f;

	/**
	 * Creates a new heap file in the given location that can accept tuples of the
	 * given type
	 * 
	 * @param f     location of the heap file
	 * @param types type of tuples contained in the file
	 */
	public HeapFile(File f, TupleDesc type) {
		// your code here
		this.td = type;
		this.f = f;
	}

	public File getFile() {
		// your code here
		return this.f;
	}

	public TupleDesc getTupleDesc() {
		// your code here
		return this.td;
	}

	/**
	 * Creates a HeapPage object representing the page at the given page number.
	 * Because it will be necessary to arbitrarily move around the file, a
	 * RandomAccessFile object
	 * should be used here.
	 * 
	 * @param id the page number to be retrieved
	 * @return a HeapPage at the given page number
	 */
	public HeapPage readPage(int id) {
		// your code here
		HeapPage hp = null;
		try {
			RandomAccessFile file = new RandomAccessFile(this.f, "r");
			byte[] heapPageContent = new byte[PAGE_SIZE];
			file.seek(id * PAGE_SIZE);
			file.read(heapPageContent);
			hp = new HeapPage(id, heapPageContent, this.getId());
			file.close();
		} catch (Exception e) {
		}
		return hp;
	}

	/**
	 * Returns a unique id number for this heap file. Consider using
	 * the hash of the File itself.
	 * 
	 * @return
	 */
	public int getId() {
		// your code here
		return this.f.hashCode();
	}

	/**
	 * Writes the given HeapPage to disk. Because of the need to seek through the
	 * file,
	 * a RandomAccessFile object should be used in this method.
	 * 
	 * @param p the page to write to disk
	 */
	public void writePage(HeapPage p) {
		// your code here
		try {
			RandomAccessFile file = new RandomAccessFile(this.f, "rw");
			file.seek(p.getId() * PAGE_SIZE);
			file.write(p.getPageData());
			file.close();
		} catch (Exception e) {
			// e.printStackTrace();
		}
	}

	/**
	 * Adds a tuple. This method must first find a page with an open slot, creating
	 * a new page
	 * if all others are full. It then passes the tuple to this page to be stored.
	 * It then writes
	 * the page to disk (see writePage)
	 * 
	 * @param t The tuple to be stored
	 * @return The HeapPage that contains the tuple
	 */
	public HeapPage addTuple(Tuple t) throws IOException {
		if (t.getDesc() != this.td) {
			return null;
		}
		// your code here
		HeapPage hp = null;
		int numPages = this.getNumPages();
		boolean inserted = false;
		for (int i = 0; i < numPages; i++) {
			hp = this.readPage(i);
			if (hp.getFirstFreeSlot() == -1) {
				continue;
			}
			try {
				hp.addTuple(t);
				inserted = true;
				break;
			} catch (Exception e1) {
			}
		}
		if (!inserted) {
			byte[] newPageData = new byte[PAGE_SIZE];
			hp = new HeapPage(numPages, newPageData, this.getId());
			try {
				hp.addTuple(t);
			} catch (Exception e2) {
			}
		}
		// this.writePage(hp);
		return hp;
	}

	/**
	 * This method will examine the tuple to find out where it is stored, then
	 * delete it
	 * from the proper HeapPage. It then writes the modified page to disk.
	 * 
	 * @param t the Tuple to be deleted
	 */
	public void deleteTuple(Tuple t) {
		// your code here
		HeapPage curr = this.readPage(t.getPid());
		try {
			curr.deleteTuple(t);
		} catch (Exception e) {
			e.printStackTrace();
		}
		this.writePage(curr);
	}

	/**
	 * Returns an ArrayList containing all of the tuples in this HeapFile. It must
	 * access each HeapPage to do this (see iterator() in HeapPage)
	 * 
	 * @return
	 */
	public ArrayList<Tuple> getAllTuples() {
		// your code here
		ArrayList<Tuple> result = new ArrayList<>();
		for (int i = 0; i < this.getNumPages(); i++) {
			Iterator<Tuple> it = this.readPage(i).iterator();
			while (it.hasNext()) {
				result.add(it.next());
			}
		}
		return result;
	}

	/**
	 * Computes and returns the total number of pages contained in this HeapFile
	 * 
	 * @return the number of pages
	 */
	public int getNumPages() {
		// your code here
		return (int) (this.f.length() / PAGE_SIZE);
	}
}
