package simpledb.lockmanager;

import simpledb.storage.PageId;
import simpledb.transaction.TransactionId;

public class PageLock {

    enum LockType { SHARE , EXCLUSIVE };

    private TransactionId _txnid;
    private LockType _lock_type;

    public PageLock(TransactionId _txnid, LockType _lock_type) {
        this._txnid = _txnid;
        this._lock_type = _lock_type;
    }

    public TransactionId get_txnid() {
        return _txnid;
    }

    public LockType get_lock_type() {
        return _lock_type;
    }


}
