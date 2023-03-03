package simpledb.lockmanager;

import simpledb.storage.PageId;
import simpledb.transaction.TransactionId;

import java.util.concurrent.ConcurrentHashMap;

public class LockManager {

    private ConcurrentHashMap<PageId, PageLock> _locks;


    private synchronized void acquire(TransactionId txnid, PageId pageid)
    {

    }

    private synchronized void release(TransactionId txnid, PageId pageid)
    {

    }

}
