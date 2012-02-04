package simpledb;

/**
 * The delete operator.  Delete reads tuples from its child operator and
 * removes them from the table they belong to.
 */
public class Delete extends AbstractDbIterator {

    private TransactionId   _txid;
    private DbIterator      _child;
    private Tuple           _tuple; // one field tuple returned by readNext
    private boolean         _done;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * @param t The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        _txid       = t;
        _child      = child;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        if (_done) return null;
        
        int count = 0;
        while (_child.hasNext()) {
            Tuple t = _child.next();
            Database.getBufferPool().deleteTuple(_txid, t);
            count++;
        }
        _done = true;
        _tuple.setField(0, new IntField(count));
        return _tuple;
    }
}
