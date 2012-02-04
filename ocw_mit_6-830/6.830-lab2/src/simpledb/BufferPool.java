package simpledb;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import com.google.common.cache.*;
/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool which check that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private int _numPages;
    private LoadingCache<PageId, Page>   mPages;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        _numPages = numPages;
        mPages = CacheBuilder.newBuilder()
                .maximumSize(numPages)
                .concurrencyLevel(1)
                .removalListener(
                    new RemovalListener<PageId, Page>() {
                        public void onRemoval(RemovalNotification<PageId, Page> notification) {
                            try {
                                flushPage(notification.getValue());
                            } catch (IOException ioe) {}
                        }
                    })
                .build(
                    new CacheLoader<PageId, Page>() {
                        public Page load(PageId pid) {
                            try {
                                DbFile dbfile = getDbFile(pid.getTableId());
                                return dbfile.readPage(pid);
                            } catch (IllegalArgumentException iae) {
                                // page doesn't exist in the file, create a new one
                                return HeapPage.createEmptyPage((HeapPageId)pid);
                            }
                        }
                    });
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        
        try {
            // This is a LRU cache, MRU is actually better for lab2 bonus workload.
            // It has been verified by an alternative implementation, but one can
            // argue that lab2 bonus workload isn't very real.
            // Microbenchmark is hard to get right.
            return mPages.get(pid);
        } catch (ExecutionException ee) {
            DbException dbe = new DbException("");
            dbe.initCause(ee);
            throw dbe;
        }
    }

    public void printHitRatio() {
        CacheStats stats = mPages.stats();
        System.out.println("BufferPool hit ratio: " + stats.hitCount() + " / " + stats.requestCount());
        System.out.println("BufferPool buf size: " + _numPages + ", " + mPages.size());
    }

    private DbFile getDbFile(int tableid) {
        return Database.getCatalog().getDbFile(tableid);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public  void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public   boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public   void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public  void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {

        DbFile dbfile = getDbFile(tableId);
        Page p = dbfile.addTuple(tid, t).get(0);
        p.markDirty(true, tid);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {

        DbFile dbfile = getDbFile(t.getRecordId().getPageId().getTableId());
        Page p = dbfile.deleteTuple(tid, t);
        p.markDirty(true, tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (Page p : mPages.asMap().values()) {
            flushPage(p);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // only necessary for lab5
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(Page p) throws IOException {
        if (p.isDirty() != null) {
            DbFile dbfile = getDbFile(p.getId().getTableId());
            dbfile.writePage(p);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2|lab3
    }
}
