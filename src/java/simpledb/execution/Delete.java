package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TupleDesc _td;
    private OpIterator _child;
//    private int _tid;
    private TransactionId _txnid;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        _txnid = t;
        _child = child;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return new TupleDesc( new Type[] {Type.INT_TYPE } );

    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        _child.open();
        super.open();

    }

    public void close() {
        // some code goes here
        _child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        _child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here

        // some code goes here
        Tuple ans = new Tuple(new TupleDesc(
                new Type[]{Type.INT_TYPE},
                new String[]{""}
        ));

        int cnt = 0;
        boolean _has_next_flag = _child.hasNext();
        if ( ! _has_next_flag) return null;
        while (_has_next_flag) {
            Tuple t = _child.next();
            try {
                Database.getBufferPool().deleteTuple(_txnid, t);
                cnt ++ ;
            } catch ( IOException e )
            {
                e.printStackTrace();
            }

            _has_next_flag = _child.hasNext();
        }
        ans.setField(0, new IntField(cnt));
        return ans;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] { _child } ;

    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        _child = children[0];
    }

}
