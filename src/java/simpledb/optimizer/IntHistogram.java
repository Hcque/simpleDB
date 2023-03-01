package simpledb.optimizer;

import simpledb.execution.OpIterator;
import simpledb.execution.Predicate;

import java.util.ArrayList;
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
    private HashMap<Double, Integer> _key_to_idx;



//ass defi
    private class Bar
    {
        public double _key;
        public int _value;
        Bar(double _k, Integer _v)
        {
            _key  = _k;
            _value = _v;

        }
    }
    private ArrayList<Bar> _y;

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
        _key_to_idx = new HashMap<>();
        _y = new ArrayList<>();

        for (int i = 0; i < _buckets; i ++ )
        {
            _y.add( new Bar(i*_interval ,0) );
            _key_to_idx.put(i * _interval, i);
        }
        // | 0 | 0 |
        // _bk = 2
    }
    public int findIndex(int v)
    {

        for (double _k: _key_to_idx.keySet()) {
            if (v >= _k) {
                return _key_to_idx.get(_k);
            }
        }
        return -1;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int i = findIndex(v);
        _y.get(i)._value ++;
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
        int i = findIndex(v);
        switch (op)
        {
            case EQUALS:
                if (v < _min || v > _max) return 0.0;
                return _y.get(i)._value * 1.0 / _count ;
            case NOT_EQUALS:
                if (v < _min || v > _max) return 1.0;
                return 1.0 - ( _y.get(i)._value * 1.0 / _count );
            case GREATER_THAN:
            case GREATER_THAN_OR_EQ:
                if (v < _min ) return 1.0;
                else if (v > _max) return 0.0;
                else {
                    double _ans = 0.0;
                    for (int j = i+1; j < _y.size(); j ++ )
                    {
                        _ans += _y.get(j)._value;
                    }
                    double _last = 0.0;
                    if (i == _y.size()-1) _last = _max;
                    else _last = _y.get(i+1)._key;
                    _ans += ( _y.get(i)._value * 1.0 * (_last - v) / _interval );
                    return _ans / _count ;
                }

            case LESS_THAN_OR_EQ:
            case LESS_THAN:
                if (v < _min ) return 0.0;
                else if (v > _max) return 1.0;
                else {
                    double _ans = 0.0;
                    for (int j = 0; j < i ; j ++ )
                    {
                        _ans += _y.get(j)._value;
                    }

                    _ans += ( _y.get(i)._value * 1.0 * (v - _y.get(i)._key) / _interval );
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
        for (Bar _b: _y) {
            ans += _b._value;
        }
        return ans / _buckets;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        String ans = "";

//        for (int e: _y)
//        {
//            ans += e + " ";
//        }
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
