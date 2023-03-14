package simpledb.lockmanager;

import simpledb.storage.PageId;
import simpledb.transaction.TransactionId;

import java.util.Map;
import java.util.HashMap;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LockManager {

    private Timer myTimer;

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
        }

        public synchronized boolean lockShared(TransactionId txnid) {
            lock.lock();

            try {
                while (writer_entered || num_readers == MAX_READERS)
                    // 等待写锁
                    _r.await();
                // 写锁等待释放（如果有）
                num_readers++;
                if (_tid_to_lock.containsKey(txnid)) throw new IllegalArgumentException();
                _tid_to_lock.put(txnid, new PageLock(PageLock.LockType.SHARE));
                lock.unlock();
                return true;
            }
            catch (InterruptedException e)
            {
                lock.unlock();
                e.printStackTrace();
            }
            lock.unlock();
            return false;
        }

        public synchronized boolean lockExclusive(TransactionId txnid) {
            try {
                lock.lock();

                if (_tid_to_lock.size() == 1 && _tid_to_lock.containsKey(txnid))
                {
                    lock.unlock();
                    return true;
                }
                while (writer_entered)
                    // 读锁等待写锁
                    _r.await(); // 停在此处， 写锁没有释放，tid insert tuples
                //
                writer_entered = true;
                while (num_readers > 0)
                    // 等待写锁
                    _w.await();

                _tid_to_lock.put(txnid, new PageLock(PageLock.LockType.EXCLUSIVE));
                lock.unlock();
                return true;
            }
            catch (InterruptedException e)
            {
                lock.unlock();
                e.printStackTrace();

            }
            return false;
        }

        public synchronized void unlockRead(TransactionId txnid)
        {
            // V operation
//            try {
                lock.lock();
            _tid_to_lock.remove(txnid);
            if (writer_entered) {
                    if (num_readers == 0) {
                        // 写等待释放
                        _w.signal();
                    }
                } else {
                    if (num_readers == MAX_READERS - 1) {
                        // careful
                        _r.signal();
                    }
                }
            lock.unlock();

//            }

        }

        public synchronized void unlockWrite(TransactionId txnid)
        {
            lock.lock();
            writer_entered = false;
            _tid_to_lock.remove(txnid);
            // 所有读等待释放
            _r.signalAll();
            lock.unlock();

        }
    }

    public static final int MAX_READERS = 10;
    public static final long MAX_LOCK_WAIT_TIME = 20000000;
    private Map<PageId, LockQue> _locks;

    public LockManager(boolean _enable_lock_cyc_detect) {
        _locks = new ConcurrentHashMap<>();
        if (_enable_lock_cyc_detect) {
            myTimer = new Timer();
            myTimer.schedule(new MyTimerTask(this), 0, 2000);
        }
    }


    private class MyTimerTask extends TimerTask {


        MyTimerTask(LockManager _lm)
        {

        }

        @Override
        public void run() {
            // 线程运行的代码
//            System.out.println( i++ );
        }
    }





    public synchronized boolean lockShared(TransactionId txnid, PageId pageid) {
        if (!_locks.containsKey(pageid)) {
            Map<TransactionId, PageLock> _locks_cur_pid = new HashMap<>();
            _locks.put(pageid, new LockQue());
        }
         return _locks.get(pageid).lockShared(txnid);

        }


    public synchronized boolean lockExclusive(TransactionId txnid, PageId pageid) throws InterruptedException {

            if (!_locks.containsKey(pageid)) {
                Map<TransactionId, PageLock> _locks_cur_pid = new HashMap<>();
                _locks.put(pageid, new LockQue());
            }
            return _locks.get(pageid).lockExclusive(txnid);
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
            _cur_que.unlockWrite(txnid);
        }
        else
        {
            _cur_que.unlockRead(txnid);
        }
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


    // methods for lock detection
    public synchronized void buildGraph()
    {

    }

    public synchronized boolean hasCycle()
    {
        return false;
    }


    public synchronized void resolveCycle()
    {
        return;
    }


}
