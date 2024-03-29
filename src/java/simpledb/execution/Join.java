package simpledb.execution;

import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    private JoinPredicate _joinPredicate;
    private OpIterator _child1;
    private OpIterator _child2;
    private TupleDesc _td;
    private ArrayList<Tuple> _answer;

    private Iterator<Tuple> _it;


    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // some code goes here
        _joinPredicate = p;
        _child1 = child1;
        _child2 = child2;
        _td = getTupleDesc();
        _answer = new ArrayList<Tuple>();

    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return _joinPredicate;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return  _td.getFieldName( _joinPredicate.getField1() );
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return  _td.getFieldName( _joinPredicate.getField2() );
    }

    /**
     * @see TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return TupleDesc.merge(_child1.getTupleDesc(), _child2.getTupleDesc());

    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        _child1.open();
        while (_child1.hasNext())
        {
            Tuple t1 = _child1.next();

            _child2.open();
            while (_child2.hasNext())
            {
                Tuple t2 = _child2.next();

                if (_joinPredicate.filter(t1, t2) == true)
                {

                    _answer.add( Tuple.concat(t1, t2) );
                }

            }
            _child2.close();
        }
        _child1.close();
        _it = _answer.iterator();
        super.open();
    }

    public void close() {
        // some code goes here

        _child1.close();
        _child2.close();
        super.close();

    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        //super.rewind();
        _it = _answer.iterator();
        _child1.rewind();
        _child2.rewind();

    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (_it.hasNext()) return _it.next();
        else return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] { _child1, _child2 };

    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this._child1 = children[0];
        this._child2 = children[1];
    }

}
