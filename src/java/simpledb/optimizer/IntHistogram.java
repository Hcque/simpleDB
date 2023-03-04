package simpledb.optimizer;

import simpledb.execution.OpIterator;
import simpledb.execution.Predicate;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int _buckets;
    private int _min;
    private int _max;

    private double _interval;
    private int _count;

//    private ArrayList<Integer> _y;
//    private HashMap<Double, Integer> _key_to_idx;


//ass defi
//    private class Bar
//    {
//        public double _key;
//        public int _value;
//        Bar(double _k, Integer _v)
//        {
//            _key  = _k;
//            _value = _v;
//
//        }
//    }
//    private ArrayList<Bar> _y;

    private int _y[];

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        _max = max;
        _min = min;
        _buckets = buckets;
        _interval = (_max - _min) * 1.0 / buckets;
//        _y = new ArrayList<>();
//        _key_to_idx = new HashMap<>();
//        _y = new ArrayList<>();

        _y = new int[buckets];

        // | 0 | 0 |
        // _bk = 2
    }
    public int findIndex(int v)
    {
//        assert v >= _min && v <= _max;
        if (v < _min || v > _max)
        {
            throw new IllegalArgumentException();
        }
        if (v == _max) return _buckets-1;
        return (int) ((v-_min) / _interval);

//        if ()

//        ArrayList<Double> _s = new ArrayList<>();
//        for (double _d: _key_to_idx.keySet()) _s.add(_d);
//        _s.sort(Comparator.naturalOrder());
//
//        System.err.println("[findIndex] max of bar: " + _s.get(_s.size()-1));
//        for (int i = 0; i < _s.size(); i ++ ) {
//            if (_s.get(i) > v)
//            {
//                assert i>= 1;
//                return i-1;
//            }
//        }
//        assert( false );
//        return -1;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int i = findIndex(v);
        _y[i] ++;
        _count ++ ;
        return;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
        int i = 0;
        switch (op)
        {
            case EQUALS:
                if (v < _min || v > _max) return 0.0;
                i = findIndex(v);
                return _y[i] * 1.0 / _count ;
            case NOT_EQUALS:
                if (v < _min || v > _max) return 1.0;
                i = findIndex(v);

                return 1.0 - ( _y[i] * 1.0 / _count );
            case GREATER_THAN:
            case GREATER_THAN_OR_EQ:
                if (v < _min ) return 1.0;
                else if (v > _max) return 0.0;
                else {
                    i = findIndex(v);
                    double _ans = 0.0;
                    for (int j = i+1; j < _y.length; j ++ )
                    {
                        _ans += _y[j];
                    }
//                    double _last = 0.0;
//                    if (i == _y.length-1) _last = _max;
//                    else _last = _y.get(i+1)._key;
//                    _ans += ( _y.get(i)._value * 1.0 * (_last - v) / _interval );
                    _ans += _y[i]*1.0 / 2;
                    return _ans / _count ;
                }

            case LESS_THAN_OR_EQ:
            case LESS_THAN:
                if (v < _min ) return 0.0;
                else if (v > _max) return 1.0;
                else {
                    i = findIndex(v);
                    double _ans = 0.0;
                    for (int j = 0; j < i ; j ++ )
                    {
                        _ans += _y[j];
                    }

                    _ans += ( _y[i] * 1.0 * (v - i*_interval) / _interval );
                    return _ans / _count ;
                }
            case LIKE:
                return 0.5;
            default:
                assert( false );
        }
        assert(false);
        return -1.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        double ans = 0.0;
        for (int i: _y) {
            ans += i;
        }
        return ans / _buckets;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        String ans = "";

        for (int e: _y)
        {
            ans += e + " ";
        }
        return ans;
    }

    class StatsContainer<Key, Value>
    {
        private ArrayList<Key> _keys;
        private ArrayList<Value> _values;
        private HashMap<Key, Integer> _key_to_idx;

        StatsContainer() {
            _keys = new ArrayList<>();
            _values = new ArrayList<>();
            _key_to_idx = new HashMap<>();

        }

        public void put(Key _k, Value _v)
        {
            _keys.add(_k);
            _values.add(_v);
            _key_to_idx.put(_k, _keys.size()-1);

        }

        public int getIndex(Key i)
        {
            return _key_to_idx.get(i);
        }


        public Value get(Key i)
        {
            return _values.get( _key_to_idx.get(i) );
        }
        public Value getViaIndex(int i)
        {
            return _values.get( i );
        }

        public int size()
        {
            return _keys.size();
        }

        public Iterator<Value> iterator() {
            return new Iterator<Value>(this);
        }
        class Iterator<T> implements java.util.Iterator<T> {
            private int cur = 0;
            private  StatsContainer _sc;
            Iterator(StatsContainer sc)
            {
                _sc = sc;
            }

            @Override
            public boolean hasNext() {
                return cur < _sc._values.size();
            }

            @Override
            public T next() {
                return (T) _sc._values.get( cur ++ );
            }


        }

    } // statsContainer cl
}
