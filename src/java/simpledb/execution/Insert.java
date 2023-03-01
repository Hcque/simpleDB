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
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

//    private TupleDesc _td;

    private OpIterator _child;
    private int _tid;
    private TransactionId _txnid;


    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        _txnid = t;
        _child = child;
        _tid = tableId;
//        _td = _child.getTupleDesc();
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
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
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
                Database.getBufferPool().insertTuple(_txnid, _tid, t);
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
