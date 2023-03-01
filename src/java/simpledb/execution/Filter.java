package simpledb.execution;

import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    private Predicate p_;
    private OpIterator child_;
    private TupleDesc td_;
    private Iterator<Tuple> iterator_;
    private ArrayList<Tuple> childList_ = new ArrayList<Tuple>();

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // some code goes here
        p_ = p;
        child_ = child;
        td_ = child_.getTupleDesc();
    }

    public Predicate getPredicate() {
        // some code goes here
        return p_;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td_;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        child_.open();
        while (child_.hasNext())
        {
            Tuple t = child_.next();
            if (p_.filter(t))
            {
                childList_.add(t);
            }
        }
        iterator_ = childList_.iterator();
        super.open();
    }
    

    public void close() {
        // some code goes here
        super.close();
        child_.close();
        iterator_ = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        iterator_ = childList_.iterator();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        if (iterator_.hasNext()) return iterator_.next();
        else throw new NoSuchElementException();
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] { child_ } ;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        child_ = children[0];
    }
}
