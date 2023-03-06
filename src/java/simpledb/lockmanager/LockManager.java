package simpledb.lockmanager;

import simpledb.storage.PageId;
import simpledb.transaction.TransactionId;

import java.util.concurrent.ConcurrentHashMap;

public class LockManager {

    private ConcurrentHashMap<PageId, PageLock> _locks;


    public LockManager() {
        _locks = new ConcurrentHashMap<>();

    }
    private synchronized void acquire(TransactionId txnid, PageId pageid, boolean readonly)
    {

        _locks.put(pageid, new PageLock(txnid, pageid, (readonly)? PageLock.LockType.SHARE: PageLock.LockType.EXCLUSIVE));
    }

    private synchronized void release(TransactionId txnid, PageId pageid)
    {
//        if (_locks.containsKey(pageid))

    }

}
