package simpledb.lockmanager;

import simpledb.common.Permissions;
import simpledb.storage.Page;
import simpledb.storage.PageId;
import simpledb.transaction.TransactionId;

import java.util.Map;
import java.util.HashMap;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LockManager {

    class LockQue
    {
        boolean writer_entered = false;
        int num_readers = 0;
        Map<TransactionId, PageLock> _tid_to_lock;

         Lock lock = new ReentrantLock(); // 账户锁
         Condition _w = lock.newCondition(); // 存款条件
         Condition _r = lock.newCondition(); //

        LockQue()
        {
            _tid_to_lock = new HashMap<>();
//            r = new Semaphore(LockManager.MAX_READERS);
//            w = new Semaphore(1);
        }


    }

    public static final int MAX_READERS = 10;
    public static final long MAX_LOCK_WAIT_TIME = 20000000;
    private Map<PageId, LockQue> _locks;

    public LockManager() {
        _locks = new ConcurrentHashMap<>();
    }


    public synchronized boolean lockShared(TransactionId txnid, PageId pageid) throws InterruptedException {

        if (!_locks.containsKey(pageid))
        {
            Map<TransactionId, PageLock> _locks_cur_pid = new HashMap<>();
            _locks.put(pageid, new LockQue());
            _locks.get(pageid).num_readers ++ ;
            _locks.get(pageid)._tid_to_lock.put(txnid, new PageLock(PageLock.LockType.SHARE));
            return true;
        }
        LockQue _cur_que = _locks.get(pageid);
        while (_cur_que.writer_entered || _cur_que.num_readers == MAX_READERS )
            synchronized (this) { _cur_que._r.wait(100); }
        _cur_que.num_readers ++ ;
        if ( _cur_que._tid_to_lock.containsKey(txnid) ) throw new IllegalArgumentException();
        _cur_que._tid_to_lock.put(txnid, new PageLock(PageLock.LockType.SHARE));
        return true;

    }

    public synchronized boolean lockExclusive(TransactionId txnid, PageId pageid) throws InterruptedException {
        if (!_locks.containsKey(pageid))
        {
            Map<TransactionId, PageLock> _locks_cur_pid = new HashMap<>();
            _locks.put(pageid, new LockQue());
            _locks.get(pageid).writer_entered = true ;
            _locks.get(pageid)._tid_to_lock.put(txnid, new PageLock(PageLock.LockType.EXCLUSIVE));
            return true;
        }
        LockQue _cur_que = _locks.get(pageid);
        while (_cur_que.writer_entered )
            synchronized (this) {_cur_que._r.wait(100); }

        _cur_que.writer_entered = true;
        while (_cur_que.num_readers > 0 )
            synchronized (this) {_cur_que._w.wait(100);}

        _locks.get(pageid)._tid_to_lock.put(txnid, new PageLock(PageLock.LockType.EXCLUSIVE));
        return true;
    }

    public synchronized boolean lockUpgrade(TransactionId txnid, PageId pageid)
    {
        return false;
    }


    public synchronized void unlock(TransactionId txnid, PageId pageid)
    {
        if (!_locks.containsKey(pageid)) throw new IllegalArgumentException();
        LockQue _cur_que = _locks.get(pageid);
        if ( _cur_que.num_readers == 0 )
        {
            unlockWrite(txnid, pageid);
        }
        else
        {
            unlockRead(txnid, pageid);
        }
    }

    public synchronized void unlockRead(TransactionId txnid, PageId pageid)
    {
        if (!_locks.containsKey(pageid)) throw new IllegalArgumentException();
        LockQue _cur_que = _locks.get(pageid);
        _cur_que.num_readers -- ;
        _cur_que._tid_to_lock.remove(txnid);
        // V operation

        if (_cur_que.writer_entered )
        {
            if (_cur_que.num_readers == 0)
            {
                _cur_que._w.notify();
            }
        }
        else
        {
            if (_cur_que.num_readers == MAX_READERS - 1)
            {
                _cur_que._r.notify();
            }
        }


    }

    public synchronized void unlockWrite(TransactionId txnid, PageId pageid)
    {
        if (!_locks.containsKey(pageid)) throw new IllegalArgumentException();
        LockQue _cur_que = _locks.get(pageid);
        _cur_que.writer_entered = false;
        _cur_que._tid_to_lock.remove(txnid);
        _locks.get(pageid)._r.notifyAll();

    }



    public synchronized boolean isHoldLock(TransactionId txnid, PageId pageid)
    {
        if (!_locks.containsKey(pageid)) return false;
        LockQue _lock_que = _locks.get(pageid);
        if (! _lock_que._tid_to_lock.containsKey(txnid)) return false;
        return true;
    }

    public synchronized void txnComplete(TransactionId txnid)
    {
        for (PageId pid: _locks.keySet())
        {
            unlock(txnid, pid);
        }
    }


}
