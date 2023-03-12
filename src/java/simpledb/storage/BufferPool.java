package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.lockmanager.LockManager;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.swing.*;
import javax.xml.crypto.Data;
import java.io.*;

import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;

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
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    private ConcurrentHashMap<PageId, Page> pages_;
    private int numPages_;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    class _Node implements Comparable<_Node>
    {
        long _ts;
        PageId _pid;

        public _Node(long _ts, PageId _pid) {
            super();
            this._ts = _ts;
            this._pid = _pid;
        }


        @Override
        public int compareTo(_Node node) {
            if (this._ts < node._ts) return -1;
            else if ( this._ts == node._ts ) return 0;
            else return 1;
        }
    }

    PriorityQueue<_Node> que = new PriorityQueue<>(); // LRU
    int _global_ts = 0;


    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        numPages_ = numPages;
        pages_ = new ConcurrentHashMap<PageId, Page>();
    }
    
    public static int getPageSize() {
        return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should b
     * e returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        boolean writeLock = false;
        if (perm == Permissions.READ_WRITE) writeLock = true;
        LockManager _lk_manager = Database.getLockManager();

        long _start = System.currentTimeMillis();

        while (true)
        {
            try {
                if (!writeLock && _lk_manager.lockShared(tid, pid)) {
                    break;
                }
                if (writeLock && _lk_manager.lockExclusive(tid, pid)) {
                    break;
                }
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
            if (System.currentTimeMillis() - _start > LockManager.MAX_LOCK_WAIT_TIME )
            {
                throw new TransactionAbortedException();

            }
        }

        if (pages_.containsKey(pid))
        {
            return pages_.get(pid);
        }
        else 
        {
            // read from disk?
            DbFile dbfile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = dbfile.readPage(pid);

            //
            if (pages_.size() == numPages_)
            {
                evictPage();
                assert numPages_ -1 == pages_.size() ;
            }

            pages_.put(pid, page);
            que.add(new _Node(_global_ts ++ , pid));

            return page;
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
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        Database.getLockManager().unlock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        Database.getLockManager().txnComplete(tid);

    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2

        return Database.getLockManager().isHoldLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2

        for (PageId pid : pages_.keySet()) {
            Page page = pages_.get(pid);
            if (commit) {
                if (page.isDirty() != null) {
                    try {
                        flushPage(page.getId());
                        page.setBeforeImage();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

            // release lock of all pages by tid

        }

    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile dbfile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pgs =  dbfile.insertTuple(tid, t);
        for (Page p: pgs)
        {
            pages_.put(p.getId(), p);
        }

    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile dbfile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pgs  = dbfile.deleteTuple(tid, t);
        for (Page p: pgs)
        {
            pages_.put(p.getId(), p);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (PageId pid: pages_.keySet() )
        {
            Page page = pages_.get(pid);
            if (page.isDirty() != null )
            {
                flushPage(page.getId());
            }
        }



    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        pages_.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        DbFile dbfile = Database.getCatalog().getDatabaseFile( pid.getTableId() );
        Page p = dbfile.readPage(pid);
        if ( p.isDirty() == null ) return;
        // WAL
        TransactionId  dirtier = p.isDirty();
        if (dirtier != null)
        {
            Database.getLogFile().logWrite(dirtier, p.getBeforeImage(), p); // undo log, write-before , physical
            Database.getLogFile().force();
        }

        dbfile.writePage( pages_.get(pid) );
        p.markDirty(false, null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (PageId pid: pages_.keySet() )
        {
            Page page = pages_.get(pid);
            if (page.isDirty() == tid)
            {
                flushPage(page.getId());
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1

        _Node _node = que.poll();
        PageId _pid = _node._pid;
        try {
            flushPage(_pid);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        pages_.remove(_pid);

    }

}
