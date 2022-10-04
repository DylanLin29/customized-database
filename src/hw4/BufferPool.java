package hw4;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

import hw1.Catalog;
import hw1.Database;
import hw1.HeapPage;
import hw1.Tuple;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches
 * a page, BufferPool which check that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
  /** Bytes per page, including header. */
  public static final int PAGE_SIZE = 4096;

  /**
   * Default number of pages passed to the constructor. This is used by
   * other classes. BufferPool should use the numPages argument to the
   * constructor instead.
   */
  public static final int DEFAULT_PAGES = 50;

  private int numPages;

  /* Use a nested class `Frame` to hold one HeapPage within the bufferPool */
  private class Frame {
    public boolean dirty;
    public Permissions perm; // the lock held by the page
    public HeapPage hp;
    public int counter; // number of transactions have the lock

    public Frame(Permissions perm, HeapPage hp) {
      this.perm = perm;
      this.hp = hp;
      this.counter = 1;
      this.dirty = false;
    }
  }

  /* The location of the heapPage within the buffer pool */
  private class Page {
    public int tableId;
    public int pid;

    public Page(int tableId, int pid) {
      this.tableId = tableId;
      this.pid = pid;
    }

    public boolean compareTo(int tableId, int pid) {
      if (this.tableId == tableId && this.pid == pid) {
        return true;
      }
      return false;
    }
  }

  /* Keep track of all the locks acquired by a transaction */
  private HashMap<Integer, ArrayList<Page>> transactions;

  /**
   * Keep track of all the transactions in cache
   * tableId -> pageId -> Frame
   */
  private HashMap<Integer, HashMap<Integer, Frame>> cache;

  /**
   * Least recently used HeapPage in the buffer pool to determine which
   * heapPage will be removed first
   * the first page in `LRU` is the least recently accessed page
   */
  private LinkedList<Page> LRU;

  /**
   * Creates a BufferPool that caches up to numPages pages.
   *
   * @param numPages maximum number of pages in this buffer pool.
   */
  public BufferPool(int numPages) {
    this.numPages = numPages;
    this.transactions = new HashMap<Integer, ArrayList<Page>>();
    this.cache = new HashMap<Integer, HashMap<Integer, Frame>>();
    this.LRU = new LinkedList<Page>();
  }

  /**
   * Retrieve the specified page with the associated permissions.
   * Will acquire a lock and may block if that lock is held by another
   * transaction.
   * <p>
   * The retrieved page should be looked up in the buffer pool. If it
   * is present, it should be returned. If it is not present, it should
   * be added to the buffer pool and returned. If there is insufficient
   * space in the buffer pool, an page should be evicted and the new page
   * should be added in its place.
   *
   * @param tid     the ID of the transaction requesting the page
   * @param tableId the ID of the table with the requested page
   * @param pid     the ID of the requested page
   * @param perm    the requested permissions on the page
   */
  public HeapPage getPage(int tid, int tableId, int pid, Permissions perm)
      throws Exception {
    HeapPage hp = null;
    // check if the page is inside the buffer pool
    if (this.cache.containsKey(tableId) && this.cache.get(tableId).containsKey(pid)) {
      Frame frame = this.cache.get(tableId).get(pid);
      int lockIdx = getLockIdx(tid, tableId, pid);
      // when the frame has lock (READ/WRITE)
      if (frame.perm != null) {
        // when the frame has a write lock and the transaction doesn't have the lock for
        // this page => this page is locked by another transaction
        if (frame.perm.permLevel == Permissions.READ_WRITE.permLevel && lockIdx == -1) {
          transactionComplete(tid, false); // abort to resolve deadlock
          throw new Exception();
        }
        // when the frame has a read lock
        if (frame.perm.permLevel == Permissions.READ_ONLY.permLevel) {
          // when the current transaction doesn't have the lock and also acquire read lock
          if (perm.permLevel == Permissions.READ_ONLY.permLevel && lockIdx == -1) {
            frame.counter++;
          }
          if (perm.permLevel == Permissions.READ_WRITE.permLevel) {
            // when the current transaction is the only transaction that has the read lock
            // => this lock can be upgraded to READ_WRITE lock
            if (lockIdx != -1 && frame.counter == 1) {
              frame.perm = Permissions.READ_WRITE;
            } else {
              transactionComplete(tid, false); // abort to resolve deadlock
              throw new Exception();
            }
          }
        }
      }
      hp = frame.hp;
    } else {
      // retrieve the heapPage from disk
      Catalog catalog = Database.getCatalog();
      hp = catalog.getDbFile(tableId).readPage(pid);

      // store the heapPage inside the cache
      Frame frame = new Frame(perm, hp);
      if (!hasSpaces()) {
        evictPage();
      }
      if (!this.cache.containsKey(tableId)) {
        this.cache.put(tableId, new HashMap<Integer, Frame>());
      }
      this.cache.get(tableId).put(pid, frame);
    }

    // if the transactions doesn't have this tid => initialized
    if (!this.transactions.containsKey(tid)) {
      this.transactions.put(tid, new ArrayList<Page>());
    }

    // if the transaction doesn't have this lock => add to current transaction
    if (getLockIdx(tid, tableId, pid) == -1) {
      this.transactions.get(tid).add(new Page(tableId, pid));
    }

    // move current page to the back
    updatePageLRU(tableId, pid);
    return hp;
  }

  /**
   * Releases the lock on a page.
   * Calling this is very risky, and may result in wrong behavior. Think hard
   * about who needs to call this and why, and why they can run the risk of
   * calling it.
   *
   * @param tid     the ID of the transaction requesting the unlock
   * @param tableID the ID of the table containing the page to unlock
   * @param pid     the ID of the page to unlock
   */
  public void releasePage(int tid, int tableId, int pid) {
    Frame frame = this.cache.get(tableId).get(pid);
    // when the page only has one lock (READ/WRITE) => remove
    if (frame.counter == 1) {
      frame.perm = null;
    } else {
      // remove the number of transactions holding READ lock
      frame.counter--;
    }
  }

  /** Return true if the specified transaction has a lock on the specified page */
  public boolean holdsLock(int tid, int tableId, int pid) {
    return getLockIdx(tid, tableId, pid) != -1;
  }

  /**
   * Commit or abort a given transaction; release all locks associated to
   * the transaction. If the transaction wishes to commit, write
   *
   * @param tid    the ID of the transaction requesting the unlock
   * @param commit a flag indicating whether we should commit or abort
   */
  public void transactionComplete(int tid, boolean commit)
      throws IOException {
    Catalog catalog = Database.getCatalog();
    Iterator i = this.transactions.get(tid).iterator();
    while (i.hasNext()) {
      Page page = (Page) i.next();
      Frame frame = this.cache.get(page.tableId).get(page.pid);
      if (frame.dirty) {
        if (commit) {
          // write the page back to disk
          flushPage(page.tableId, page.pid);
        } else {
          // read the page from the disk to buffer pool to cover the changes
          frame.hp = catalog.getDbFile(page.tableId).readPage(page.pid);
        }
      }
      releasePage(tid, page.tableId, page.pid);
      // remove the current lock from transaction `tid`
      i.remove();
    }
  }

  /**
   * Add a tuple to the specified table behalf of transaction tid. Will
   * acquire a write lock on the page the tuple is added to. May block if the lock
   * cannot be acquired.
   * 
   * Marks any pages that were dirtied by the operation as dirty
   *
   * @param tid     the transaction adding the tuple
   * @param tableId the table to add the tuple to
   * @param t       the tuple to add
   */
  public void insertTuple(int tid, int tableId, Tuple t)
      throws Exception {
    Catalog catalog = Database.getCatalog();
    HeapPage hp = catalog.getDbFile(tableId).addTuple(t);
    int pid = hp.getId();
    Frame frame = cache.get(tableId).get(pid);
    int lockIdx = this.getLockIdx(tid, tableId, pid);
    if (lockIdx == -1 || frame.perm == null ||
        frame.perm.permLevel == Permissions.READ_ONLY.permLevel) {
      throw new Exception();
    }
    frame.dirty = true;
    frame.hp = hp;
  }

  /**
   * Remove the specified tuple from the buffer pool.
   * Will acquire a write lock on the page the tuple is removed from. May block if
   * the lock cannot be acquired.
   *
   * Marks any pages that were dirtied by the operation as dirty.
   *
   * @param tid     the transaction adding the tuple.
   * @param tableId the ID of the table that contains the tuple to be deleted
   * @param t       the tuple to add
   */
  public void deleteTuple(int tid, int tableId, Tuple t)
      throws Exception {
    int pid = t.getPid();
    HeapPage hp = getPage(tid, tableId, pid, Permissions.READ_WRITE);
    hp.deleteTuple(t);
    // set the page to be dirty
    this.cache.get(tableId).get(pid).dirty = true;
  }

  private synchronized void flushPage(int tableId, int pid) throws IOException {
    HeapPage hp = this.cache.get(tableId).get(pid).hp;
    Catalog catalog = Database.getCatalog();
    catalog.getDbFile(tableId).writePage(hp);
  }

  /**
   * Discards a page from the buffer pool.
   * Flushes the page to disk to ensure dirty pages are updated on disk.
   */
  private synchronized void evictPage() throws Exception {
    Iterator i = this.LRU.iterator();
    while (i.hasNext()) {
      Page page = (Page) i.next();
      if (!this.cache.get(page.tableId).get(page.pid).dirty) {
        i.remove();
        this.cache.get(page.tableId).remove(page.pid);
        return;
      }
    }
    throw new Exception();
  }

  private boolean hasSpaces() {
    return this.LRU.size() < this.numPages;
  }

  private int getLockIdx(int tid, int tableId, int pid) {
    ArrayList<Page> pages = this.transactions.get(tid);
    if (pages != null) {
      int i = 0;
      for (Page page : this.transactions.get(tid)) {
        if (page.compareTo(tableId, pid)) {
          return i;
        }
        i++;
      }
    }
    return -1;
  }

  private void updatePageLRU(int tableId, int pid) {
    Iterator i = this.LRU.iterator();
    boolean found = false;
    while (i.hasNext()) {
      Page page = (Page) i.next();
      if (page.compareTo(tableId, pid)) {
        i.remove();
        this.LRU.add(page);
        found = true;
        break;
      }
    }
    if (!found) {
      this.LRU.add(new Page(tableId, pid));
    }
  }
}
