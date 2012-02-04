package simpledb;
import java.util.*;
import java.io.*;

/**
 * Inserts tuples read from the child operator into
 * the tableid specified in the constructor
 */
public class Insert extends AbstractDbIterator {

    private TransactionId   _txid;
    private DbIterator      _child;
    private int             _tableid;
    private Tuple           _tuple; // one field tuple returned by readNext
    private boolean         _done;

    /**
     * Constructor.
     * @param t The transaction running the insert.
     * @param child The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
        throws DbException {
        _txid       = t;
        _child      = child;
        _tableid    = tableid;
        _done       = false;
        _tuple      = new Tuple(new TupleDesc(new Type[]{Type.INT_TYPE}));
    }

    public TupleDesc getTupleDesc() {
        return _tuple.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
        _done = false;
        _child.open();
    }

    public void close() {
        _done = true;
        _child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        _done = false;
        _child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool.
     * An instances of BufferPool is available via Database.getBufferPool().
     * Note that insert DOES NOT need check to see if a particular tuple is
     * a duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
    * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple readNext()
            throws TransactionAbortedException, DbException {

        if (_done) return null;
        
        int count = 0;
        while (_child.hasNext()) {
            Tuple t = _child.next();
            try {
                Database.getBufferPool().insertTuple(_txid, _tableid, t);
                count++;
            } catch (IOException ioe) {
                DbException dbe = new DbException("");
                dbe.initCause(ioe);
                throw dbe;
            }
        }
        _done = true;
        _tuple.setField(0, new IntField(count));
        return _tuple;
    }
}
