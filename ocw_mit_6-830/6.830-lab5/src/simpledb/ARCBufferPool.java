package simpledb;

import java.io.IOException;
import java.util.*;

/**
 * Implements Adaptive Replacement Cache (ARC) algorithm:
 *      http://www.usenix.org/publications/login/2003-08/pdfs/Megiddo.pdf
 */
public class ARCBufferPool extends BufferPool {

    private int                 mNumPages;
    private int                 mMRUTargetLen;
    private Map<PageId, Page>   mMRU;
    private Deque<PageId>       mMRUGhost;
    private Map<PageId, Page>   mMFU;
    private Deque<PageId>       mMFUGhost;

    private int     mBufferHits;
    private int     mBufferMisses;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public ARCBufferPool(int numPages) {
        super(numPages);
        mNumPages   = numPages;
        mMRU        = new LinkedHashMap<PageId, Page>(numPages / 2, 0.75f);
        mMRUGhost   = new ArrayDeque<PageId>();
        mMFU        = new LinkedHashMap<PageId, Page>(numPages / 2, 0.75f);
        mMFUGhost   = new ArrayDeque<PageId>();
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
        
        Page page = null;
        if (mMRU.containsKey(pid)) {
            mBufferHits++;
            // hit mru list: move it to the head of mfu list
            page = mMRU.remove(pid);
            mMFU.put(pid, page);
        } else if (mMFU.containsKey(pid)) {
            mBufferHits++;
            // hit mfu list: also move it to the head of mfu list
            page = mMFU.remove(pid);
            mMFU.put(pid, page);
        } else if (mMRUGhost.contains(pid)) {
            mBufferMisses++;
            // hit mru ghost list: mru_add it to mfu list
            increaseMRUTarget();
            mMRUGhost.remove(pid);
            evictOnePage();
            page = getNewPage(pid);
            mMFU.put(pid, page);
        } else if (mMFUGhost.contains(pid)) {
            mBufferMisses++;
            // hit mfu ghost list: mru_add it to mfu list
            decreaseMRUTarget();
            mMFUGhost.remove(pid);
            evictOnePage();
            page = getNewPage(pid);
            mMFU.put(pid, page);
        } else {
            mBufferMisses++;
            // not hit anything
            maybeEvict();
            page = getNewPage(pid);
            mMRU.put(pid, page);
        }

        return page;
    }

    private void increaseMRUTarget() {
        if (mMRUGhost.size() > 0)
            mMRUTargetLen += Math.max(mMFUGhost.size() / mMRUGhost.size(), 1);
        else
            mMRUTargetLen++;
        mMRUTargetLen = Math.min(mMRUTargetLen, mNumPages);
    }

    private void decreaseMRUTarget() {
        if (mMFUGhost.size() > 0)
            mMRUTargetLen -= Math.max(mMRUGhost.size() / mMFUGhost.size(), 1);
        else
            mMRUTargetLen--;
        mMRUTargetLen = Math.max(mMRUTargetLen, 0);
    }

    private void maybeEvict() throws DbException {
        if (mMRU.size() + mMRUGhost.size() >= mNumPages) { // mMRU + mMRUGhost is full
            if (mMRU.size() < mNumPages) { // still has room in mMRU, lru_remove mMRUGhost
                mMRUGhost.removeLast();
                evictOnePage();
            } else { // has no room in mMRU, mMRUGhost must be empty, lru_remove mMRU
                PageId lru_pid = (PageId)mMRU.keySet().toArray()[0];
                flushPageRethrowException(mMRU.remove(lru_pid));
            }
        } else {
            int all_entries_count = mMRU.size() + mMRUGhost.size() + mMFU.size() + mMFUGhost.size();
            if (all_entries_count >= mNumPages) {
                // Cache is full
                if (all_entries_count >= mNumPages * 2) {
                    // Directory is full, lru_remove mMFUGhost
                    mMFUGhost.removeLast();
                }
                evictOnePage();
            }
        }
    }

    private void evictOnePage() throws DbException {
        PageId pid;
        Page page;
        if (mMRU.size() >= Math.max(1, mMRUTargetLen)) {
            // lru_remove mMRU and mru_add it on mMRUGhost
            pid = (PageId)mMRU.keySet().toArray()[0];
            page = mMRU.remove(pid);
            mMRUGhost.addFirst(pid);
        } else {
            // lru_remove mMFU and mru_add it on mMFUGhost
            pid = (PageId)mMFU.keySet().toArray()[0];
            page = mMFU.remove(pid);
            mMFUGhost.addFirst(pid);
        }
        flushPageRethrowException(page);
    }

    private void flushPageRethrowException(Page page) throws DbException {
        try {
            flushPage(page);
        } catch (IOException ioe) {
            DbException dbe = new DbException("");
            dbe.initCause(ioe);
            throw dbe;
        }
    }

    private Page getNewPage(PageId pid) {
        Page newpage;
        DbFile dbfile = getDbFile(pid.getTableId());
        try {
            newpage = dbfile.readPage(pid);
        } catch (IllegalArgumentException iae) {
            // page doesn't exist in the file, create a new one
            newpage = HeapPage.createEmptyPage((HeapPageId)pid);
        }
        
        return newpage;
    }

    public void printHitRatio() {
        int total = mBufferHits + mBufferMisses;
        System.out.println("ARCBufferPool hit ratio: " + mBufferHits + " / " + total);
        System.out.println("ARCBufferPool mru size: " + mMRU.size() + ", " + mMRUGhost.size() + ", " + mMRUTargetLen);
        System.out.println("ARCBufferPool mfu size: " + mMFU.size() + ", " + mMFUGhost.size());
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
        for (PageId pid : mMRU.keySet()) {
            flushPage(mMRU.get(pid));
        }
        for (PageId pid : mMFU.keySet()) {
            flushPage(mMFU.get(pid));
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
