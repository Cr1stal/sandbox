package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntAggregator implements Aggregator {

    // case class please!
    private InternalAggregator   _internal_aggregator;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what the aggregation operator
     */

    public IntAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (gbfield != NO_GROUPING)
            _internal_aggregator = new GroupingIA(gbfield, gbfieldtype, afield, what);
        else
            _internal_aggregator = new NoGroupingIA(gbfield, gbfieldtype, afield, what);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void merge(Tuple tup) {
        _internal_aggregator.merge(tup);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        return _internal_aggregator.iterator();
    }

    private abstract class InternalAggregator {
    
        protected int   _gbfield;
        protected Type  _gbtype;
        protected int   _afield;
        protected Op    _op;
        protected int   _defaultvalue;

        public InternalAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
            _gbfield    = gbfield;
            _gbtype     = gbfieldtype;
            _afield     = afield;
            _op         = what;

            switch (what) {
            case MIN:
                _defaultvalue = Integer.MAX_VALUE;
                break;
            case MAX:
                _defaultvalue = Integer.MIN_VALUE;
                break;
            default:
                _defaultvalue = 0;
            }
        }

        public abstract void merge(Tuple tup);
        public abstract DbIterator iterator();

        protected Integer[] mergeInternal(Integer v, Integer v1, Integer v2) {
            switch (_op) {
            case MIN:
                v1 = Math.min(v1, v);
                break;
            case MAX:
                v1 = Math.max(v1, v);
                break;
            case SUM:
                v1 += v;
                break;
            case COUNT:
                v1++;
                break;
            case AVG:
                v1 += v;
                v2 ++;
                break;
            }

            return new Integer[]{v1, v2};
        }
    }

    private class GroupingIA extends InternalAggregator {
        
        // Aggregated values for group-by
        // Aggregator.AVG needs to remember both sum and count, so two maps
        Map<Field, Integer> _values1, _values2;

        public GroupingIA(int gbfield, Type gbfieldtype, int afield, Op what) {
            super(gbfield, gbfieldtype, afield, what);
            _values1 = new HashMap<Field, Integer>();
            _values2 = new HashMap<Field, Integer>();
        }

        public void merge(Tuple tup) {

            int value = ((IntField)tup.getField(_afield)).getValue();
            Field gb = tup.getField(_gbfield);
            Integer v1 = _values1.containsKey(gb) ? _values1.get(gb) : _defaultvalue;
            Integer v2 = _values2.containsKey(gb) ? _values2.get(gb) : _defaultvalue;

            Integer[] newvalues = mergeInternal(value, v1, v2);
            _values1.put(gb, newvalues[0]);
            _values2.put(gb, newvalues[1]);
        }

        public DbIterator iterator() {
            return new GroupingIAIterator(this);
        }
    }

    private class NoGroupingIA extends InternalAggregator {
        
        // Aggregated value for NO_GROUPING
        // Again, Aggregator.AVG needs both sum and count
        int     _value1, _value2;

        public NoGroupingIA(int gbfield, Type gbfieldtype, int afield, Op what) {
            super(gbfield, gbfieldtype, afield, what);
            _value1 = _defaultvalue;
            _value2 = _defaultvalue;
        }

        public void merge(Tuple tup) {
            int value = ((IntField)tup.getField(_afield)).getValue();
            Integer[] newvalues = mergeInternal(value, _value1, _value2);
            _value1 = newvalues[0];
            _value2 = newvalues[1];
        }

        public DbIterator iterator() {
            return new NoGroupingIAIterator(this);
        }
    }

    private abstract class AbstractIADbIterator extends AbstractDbIterator {
        
        protected int computeNext(int v1, int v2, Op what) {
            int value = 0;

            switch (what) {
            case MAX:
            case MIN:
            case SUM:
            case COUNT:
                value = v1;
                break;
            case AVG:
                value = v1 / v2;
                break;
            }

            return value;
        }
    }

    private class GroupingIAIterator extends AbstractIADbIterator {

        private GroupingIA                  _ia;
        private TupleDesc                   _tupledesc;
        private Iterator<? extends Field>   _gbfields;  // iterator over goup-by fields

        public GroupingIAIterator(GroupingIA ia) { 
            _ia = ia;
            _tupledesc = new TupleDesc(new Type[]{_ia._gbtype, Type.INT_TYPE});
        }
        
        protected Tuple readNext() throws DbException,TransactionAbortedException
        {
            Tuple t = new Tuple(_tupledesc);

            if (_gbfields.hasNext()) {
                Field gbfield = _gbfields.next();
                int value = computeNext(_ia._values1.get(gbfield),
                                        _ia._values2.get(gbfield),
                                        _ia._op);

                t.setField(0, gbfield);
                t.setField(1, new IntField(value));
                return t;
            }

            return null;
        }

        public TupleDesc getTupleDesc() {
            return _tupledesc;
        }

        public void rewind() throws DbException, TransactionAbortedException
        {
            open();
        }
        
        public void open()
            throws DbException, TransactionAbortedException
        {
            _gbfields = _ia._values1.keySet().iterator();
        }
    }

    private class NoGroupingIAIterator extends AbstractIADbIterator {

        private NoGroupingIA    _ia;
        private TupleDesc       _tupledesc;
        private boolean         _iterated;  // NO_GROUPING iterator only has one value

        public NoGroupingIAIterator(NoGroupingIA ia) { 
            _ia = ia;
            _tupledesc = new TupleDesc(new Type[]{Type.INT_TYPE});
            _iterated = false;
        }
        
        protected Tuple readNext() throws DbException,TransactionAbortedException
        {
            Tuple t = new Tuple(_tupledesc);

            if (!_iterated) {
                int value = computeNext(_ia._value1, _ia._value2, _ia._op);

                t.setField(0, new IntField(value));
                _iterated = true;
                return t;
            }

            return null;
        }

        public TupleDesc getTupleDesc() {
            return _tupledesc;
        }

        public void rewind() throws DbException, TransactionAbortedException
        {
            _iterated = false;
        }
        
        public void open()
            throws DbException, TransactionAbortedException
        {
            _iterated = false;
        }
    }
}
