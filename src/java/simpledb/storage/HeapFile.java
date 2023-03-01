package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File file;
    private TupleDesc tupledesc;
    private int id;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        tupledesc = td;
        id = f.getAbsoluteFile().hashCode(); // assign a id
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        // throw new UnsupportedOperationException("implement this");
        return id;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupledesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
//        HeapPageId id = new HeapPageId(pid.getTableId(), pid.getPageNumber());
        int pageNo = pid.getPageNumber();
        int pageSz = BufferPool.getPageSize();
        byte[] data = new byte[pageSz];
        RandomAccessFile rf = null;
        Page ans = null;

        try {
            rf = new RandomAccessFile(file, "r");
            rf.seek(pageNo * pageSz);
            rf.read(data, 0, pageSz);
            ans = new HeapPage(new HeapPageId(pid.getTableId(), pageNo), data) ;
        }
        catch (IOException e)
        {
            e.printStackTrace();
        } finally {
            try {
                rf.close();
            } catch (IOException e)
            {
                e.printStackTrace();
            }
        }
        return ans;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        RandomAccessFile rf =  new RandomAccessFile(file, "rw");
        int pageNo = page.getId().getPageNumber();
        int pageSz = BufferPool.getPageSize();
        rf.seek(pageNo * pageSz);
        rf.write(page.getPageData());
        rf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        long flen = file.length();
        int plen = BufferPool.getPageSize();
        if (flen % plen == 0) return (int) (flen / (long)plen );
        else return (int) (flen / (long)plen + 1 );
    }

//    public int numTuples() {
//        // some code goes here
//        int ans = 0;
//        for (int i = 0;i < numPages(); i ++ )
//        {
//            HeapPageId pid = new HeapPageId(id, numPages()-1);
//            HeapPage page = (HeapPage) Database.getBufferPool().getPage(id, pid, Permissions.READ_ONLY);
//
////            ans +=
//        }
//    }


    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> ans = new ArrayList<>();
        HeapPageId pid = new HeapPageId(id, numPages()-1);
//        HeapPageId pid = (HeapPageId)t.getRecordId().getPageId();
//        assert ( id == t.getRecordId().getPageId().getTableId() );
//        pid.setTableId(id);
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);

        if (page.getNumEmptySlots() == 0)
        {
            HeapPage page_new = new HeapPage(new HeapPageId(id, pid.getPageNumber()+1), page.createEmptyPageData() );
            page_new.insertTuple(t);
            ans.add(page_new);
            writePage(page_new); // Directly flush to disk
            return ans;
        }
        page.insertTuple(t);
        ans.add(page);
        return ans;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        ArrayList<Page> ans = new ArrayList<>();
        PageId pid = t.getRecordId().getPageId();

        //
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);

        //HeapPage page = (HeapPage) readPage(pid);

        page.deleteTuple(t);
//        if ()
        ans.add(page);
        return ans;


        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }

//    public int numTuples()
//    {
//
//
//    }

    class HeapFileIterator implements DbFileIterator
    {
        private Iterator<Tuple> cur_iterator;
        private int cur_pid;
        private TransactionId txnid_;
        private HeapFile heapfile_;

        HeapFileIterator(HeapFile heapfile, TransactionId txnid)
        {
            heapfile_ = heapfile;
            txnid_ = txnid;
        }
    public void open()
        throws DbException, TransactionAbortedException
        {
            cur_pid = 0;
            Page page = Database.getBufferPool().getPage(txnid_, new HeapPageId(heapfile_.getId(), 0), Permissions.READ_ONLY);
            HeapPage hpage = (HeapPage)page;
            cur_iterator = hpage.iterator();
        }


    public boolean hasNext()
        throws DbException, TransactionAbortedException
        {
            if (cur_iterator == null ) return false;
            if (cur_iterator.hasNext()) return true;
            else
            {
                if (cur_pid < heapfile_.numPages()-1)
                {
                    cur_pid ++ ;
                    Page page = Database.getBufferPool().getPage(txnid_, new HeapPageId(heapfile_.getId(), cur_pid), Permissions.READ_ONLY);
                    HeapPage hpage = (HeapPage)page;
                    cur_iterator = hpage.iterator();
                    return true;
                }
                else return false;
            }
        }

    public Tuple next()
        throws DbException, TransactionAbortedException, NoSuchElementException
        {
            if (hasNext()) return cur_iterator.next();
            else throw new NoSuchElementException();
        }


    public void rewind() throws DbException, TransactionAbortedException
    {
            cur_pid = 0;
            Page page = Database.getBufferPool().getPage(txnid_, new HeapPageId(heapfile_.getId(), cur_pid), Permissions.READ_ONLY);
            HeapPage hpage = (HeapPage)page;
            cur_iterator = hpage.iterator();
    }

    public void close()
    {
        cur_iterator = null;
    }


    }

}

