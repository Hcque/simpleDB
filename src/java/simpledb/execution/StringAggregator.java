package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;
import simpledb.storage.Field;

import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int _gbfield;
    private Type _gbfieldtype;
    private int _afield;
    private Op _what;

    private int _count = 0;

    private HashMap<Field, Integer> _c_map = new HashMap<>();


    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        _gbfield = gbfield;
        _gbfieldtype = gbfieldtype;
        _afield = afield;
        _what = what;

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (NO_GROUPING == _gbfield)
        {
            _count ++ ;
        }
        else
        {
            Field _k = tup.getField(_gbfield);
            if ( ! _c_map.containsKey(_k))
            {
                _c_map.put(_k, 1);
            }
            else
            {
                _c_map.put(_k, _c_map.get(_k) + 1);
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
//        throw new UnsupportedOperationException("please implement me for lab2");
        return new StringOpIterator(this);

    }


    class StringOpIterator implements OpIterator
    {
        private StringAggregator _agg;
        private ArrayList<Tuple> _answer;
        private Iterator<Tuple> _it;

        StringOpIterator(StringAggregator agg)
        {
            _agg = agg;
            _answer = new ArrayList<>();
        }
        @Override
        public void open() throws DbException, TransactionAbortedException {
            if (NO_GROUPING == _gbfield)
            {
                TupleDesc _td = getTupleDesc();
                Tuple _t = new Tuple(_td  );
                _t.setField(0, new IntField(_count));
                _answer.add( _t );

            }
            else {
                TupleDesc _td = getTupleDesc();
                for (Field i : _c_map.keySet())
                {
                    Tuple _t = new Tuple(_td  );
                        _t.setField( 0,  i );
                    _t.setField( 1,  new IntField( _c_map.get(i) )  );
                    _answer.add( _t );
                }

            }
            _it = _answer.iterator();
//            super.open();

        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return _it.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (hasNext()) return _it.next();
            else throw new NoSuchElementException();

        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            _it = _answer.iterator();

        }

        @Override
        public TupleDesc getTupleDesc() {
            if (NO_GROUPING == _gbfield) {
                TupleDesc _td = new TupleDesc(
                        new Type[]{Type.INT_TYPE},
                        new String[]{""}
                );
                return _td;
            }
            else {
                TupleDesc _td = new TupleDesc(
                        new Type[]   {_gbfieldtype,  Type.INT_TYPE},
                        new String[] {"", ""}
                );
                return _td;

            }
        }

        @Override
        public void close() {
//            _it.close();

            _it = null;

        }
    }

}
