package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.io.File;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;


/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private  int _gbfield;
    private Type _gbfieldtype;
    private int _afield;
    private Op _what;
    private Integer _count = 0;
    private Integer _sum = 0;
    private Integer _avg = 0;
    private Integer _min = 0;
    private Integer _max = 0;

    private HashMap<Field, Integer> _count_map = new HashMap<>();
    private HashMap<Field, Integer> _sum_map = new HashMap<>();
    private HashMap<Field, Integer> _min_map = new HashMap<>();
    private HashMap<Field, Integer> _max_map = new HashMap<>();
    private HashMap<Field, Integer> _avg_map = new HashMap<>();

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        _gbfield = gbfield;
        _gbfieldtype = gbfieldtype;
        _afield = afield;
        _what = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {

        // some code goes here

        Field afield = tup.getField(_afield);

        Integer _v = ((IntField)tup.getField(_afield)).getValue();

        if (NO_GROUPING == _gbfield) {

            _count++;
            _sum += _v;
            switch (_what) {
                case MIN:
                    if (_count == 0) {
                        _min = _v;
                    } else {
                        _min = Math.min(_min, _v);
                    }
                    break;

                case AVG:
                    break;

                case MAX:
                    if (_count == 0) {
                        _max = _v;
                    } else {
                        _max = Math.max(_max, _v);
                    }
                    break;
            }
        }
        else // grouping terms , store with hashMap
        {

            Field gbfield = tup.getField(_gbfield);
            Field _key = tup.getField(_gbfield);

            if ( ! _count_map.containsKey(_key))
            {
                _count_map.put(_key, 1);
            }
            else {
                _count_map.put(_key, _count_map.get(_key) + 1 );
            }

            if ( ! _sum_map.containsKey(_key))
            {
                _sum_map.put(_key, _v);
            }
            else {
                _sum_map.put(_key, _sum_map.get(_key) + _v);
            }

            switch (_what) {
                case MIN:
                    if ( ! _min_map.containsKey(_key))
                    {
                        _min_map.put(_key, _v);
                    }
                    else {
                        _min_map.put(_key, Math.min(_min_map.get(_key), _v));
                    }
                    break;

                case AVG:
                    if ( ! _avg_map.containsKey(_key))
                    {
                        _avg_map.put(_key, _v);
                    }
                    else {
                        _avg_map.put(_key, _sum_map.get(_key) / _count_map.get(_key) );
                    }
                    break;

                case MAX:
                    if ( ! _max_map.containsKey(_key))
                    {
                        _max_map.put(_key, _v);
                    }
                    else {
                        _max_map.put(_key, Math.max(_max_map.get(_key), _v));
                    }
            }
        }


    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
//        throw new
//        UnsupportedOperationException("please implement me for lab2");

        return new IntIterator(this);

    }

    class IntIterator implements OpIterator
    {
        private IntegerAggregator _agg;
        private Iterator<Tuple> _it;
        private ArrayList<Tuple> _answer;
        IntIterator(IntegerAggregator agg)
        {
            _agg = agg;
            _answer = new ArrayList<>();
            //_it = _answer.iterator();

        }
        @Override
        public void open() throws DbException, TransactionAbortedException {
            if (NO_GROUPING == _gbfield) {
                _agg._avg = _agg._sum / _agg._count;
                Tuple _t = new Tuple(getTupleDesc());
                int _v = 0;

                switch (_agg._what) {
                    case MIN:
                        _v = _agg._min;
                        break;
                    case MAX:
                        _v = _agg._max;
                        break;
                    case SUM:
                        _v = _agg._sum;
                        break;
                    case COUNT:
                        _v = _agg._count;
                        break;
                    case AVG:
                        _v = _agg._avg;
                        break;

                }
                _t.setField(0, new IntField(_v));

                _answer.add(_t);
                _it = _answer.iterator();
            }
            else
            {
                HashMap<Field, Integer> _tmp = null;

                switch (_agg._what) {
                    case MIN:
                        _tmp = _min_map;
                        break;
                    case MAX:
                        _tmp = _max_map;
                        break;
                    case SUM:
                        _tmp = _sum_map;
                        break;
                    case COUNT:
                        _tmp = _count_map;
                        break;
                    case AVG:
                        _tmp = _avg_map;
                        break;
                }
                for (Field _f  : _tmp.keySet() ) {
                    Tuple _t = new Tuple(getTupleDesc());
                    _t.setField(0, _f);
                    _t.setField(1, new IntField(_tmp.get(_f)));
                    _answer.add(_t);
                }
                _it = _answer.iterator();
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (_it.hasNext()) return true;
            return false;
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
                Type[] _t_a = {Type.INT_TYPE};
                String[] _f_a = {""};
                return new TupleDesc(_t_a, _f_a);
            }
            else
            {
                Type[] _t_a = {_gbfieldtype, Type.INT_TYPE};
                String[] _f_a = {"", ""};
                return new TupleDesc(_t_a, _f_a);
            }
        }

        @Override
        public void close() {
//            _it = null;

        }
    }

}
