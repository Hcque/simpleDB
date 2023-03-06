package simpledb.lockmanager;

import simpledb.storage.PageId;
import simpledb.transaction.TransactionId;

public class PageLock {

    enum LockType { SHARE , EXCLUSIVE };

    private TransactionId _txnid;


    private PageId _pageid;
    private LockType _lock_type;

    public PageLock(TransactionId _txnid, PageId _pageid, LockType _lock_type) {
        this._txnid = _txnid;
        this._pageid = _pageid;
        this._lock_type = _lock_type;
    }

    public TransactionId get_txnid() {
        return _txnid;
    }


    public PageId get_pageid() {
        return _pageid;
    }

    public LockType get_lock_type() {
        return _lock_type;
    }


}
