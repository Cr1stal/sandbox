package simpledb;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;
import com.google.common.collect.*;

public class PageLock {

	private Table<TransactionId, PageId, Permissions>			mLocks;
	private Table<TransactionId, TransactionId, Set<PageId>>	mDepGraph;
	
	private ReentrantLock			mGuard;
	private Map<PageId, Condition>	mPageGuards;

	public PageLock() {
		mLocks		= HashBasedTable.create();
		mDepGraph 	= HashBasedTable.create();

		mGuard 		= new ReentrantLock();
		mPageGuards	= new HashMap<PageId, Condition>();
	}
	

	public void lock(TransactionId tid, PageId pid, Permissions perm)
		throws TransactionAbortedException
	{
		mGuard.lock();
		try {
	        Permissions current_perm = getCurrentPerm(pid);
	        if (current_perm == null) {
	            // Page has no lock, grant whatever lock the transaction asks.
	            grantLock(tid, pid, perm);
	        } else if (getLockCount(pid) == 1 && isLockHoldBy(tid, pid)) {
	            // The transaction is the only one holds a lock on the page,
	            // grant whatever lock it asks. May upgrade to exclusive lock in the process.
	            grantLock(tid, pid, perm);
	        } else if (isSharedLock(current_perm) && isSharedLock(perm)) {
	            // Multiple transactions can have a shared lock, grant it.
	            grantLock(tid, pid, Permissions.READ_ONLY);
	        } else {
	            // There may have deadlock. If not, block.
	            abortIfDeadlocked(tid, pid, perm);
	           	addDependencies(tid, pid, perm);
	            if (isSharedLock(perm)) {
	            	// Block as a reader
	            	while (!canGrantSharedLock(tid, pid)) {
	            		await(pid);
	            	}
	            	grantLock(tid, pid, Permissions.READ_ONLY);
	            } else {
	            	// Block as a writer
	            	while (!canGrantExclusiveLock(tid, pid)) {
	            		await(pid);
	            		abortIfDeadlocked(tid, pid, perm);
	            		addDependencies(tid, pid, perm);
	            	}
	            	grantLock(tid, pid, Permissions.READ_WRITE);
	            }
	        }
		} finally {
			mGuard.unlock();
		}
	}

	private Permissions getCurrentPerm(PageId pid) {
		Map<TransactionId, Permissions> perms = mLocks.column(pid);
		if (perms.isEmpty()) return null;
		return perms.values().iterator().next();
	}

	private void grantLock(TransactionId tid, PageId pid, Permissions perm) {
		Permissions current_perm = mLocks.get(tid, pid);
		if (current_perm == Permissions.READ_WRITE && perm == Permissions.READ_ONLY) // Never downgrade
			return;
		mLocks.put(tid, pid, perm);
	}

	private void revokeLock(TransactionId tid, PageId pid) {
		mLocks.remove(tid, pid);
		removeDependencies(tid, pid);
	}

	private int getLockCount(PageId pid) {
		Map<TransactionId, Permissions> perms = mLocks.column(pid);
		return perms.size();
	}

	private boolean isLockHoldBy(TransactionId tid, PageId pid) {
		Map<TransactionId, Permissions> perms = mLocks.column(pid);
		if (perms == null) return false;
		return perms.keySet().contains(tid);
	}

	private boolean isSharedLock(Permissions perm) {
		return perm == Permissions.READ_ONLY;
	}

	private boolean canGrantSharedLock(TransactionId tid, PageId pid) {
		return getCurrentPerm(pid) == null
			   || getCurrentPerm(pid) == Permissions.READ_ONLY
			   || isLockHoldBy(tid, pid);
	}

	private boolean canGrantExclusiveLock(TransactionId tid, PageId pid) {
		return getCurrentPerm(pid) == null
			   || (getLockCount(pid) == 1 && isLockHoldBy(tid, pid));
	}

    private void addDependencies(TransactionId tid, PageId pid, Permissions perm) {
	    if (perm == Permissions.READ_WRITE || isExclusivelyLocked(pid)) {
    	    for (TransactionId txid : mLocks.column(pid).keySet()) {
    	    	if (!tid.equals(txid)) {
			        Set<PageId> deps = mDepGraph.get(tid, txid);
			        if (deps == null) deps = new HashSet<PageId>();
                	deps.add(pid);
		            mDepGraph.put(tid, txid, deps);
                }
            }
        }
    }

    private void removeDependencies(TransactionId tid, PageId pid) {
    	Set<TransactionId> transactions = new HashSet<TransactionId>();
    	transactions.addAll(mDepGraph.row(tid).keySet());
    	for (TransactionId txid : transactions) {
    		Set<PageId> deps = mDepGraph.get(tid, txid);
    		if (deps != null) {
    			deps.remove(pid);
    			if (deps.size() == 0)
    				mDepGraph.remove(tid, txid);
    			else
    				mDepGraph.put(tid, txid, deps);
    		}
    	}
    }

	private void abortIfDeadlocked(TransactionId tid, PageId pid, Permissions perm)
		throws TransactionAbortedException
	{
        Deque<TransactionId> stack = new ArrayDeque<TransactionId>();
        if (perm == Permissions.READ_WRITE || isExclusivelyLocked(pid)) {
          	stack.addAll(mLocks.column(pid).keySet());
        }

        while (!stack.isEmpty()) {
            Set<TransactionId> deps = mDepGraph.row(stack.removeFirst()).keySet();
            if (deps != null) {
                if (deps.contains(tid)) {
                	unlockAll(tid);
                    throw new TransactionAbortedException();
                } else {
                    stack.addAll(deps);
                }
            }
        }
	}

	private boolean isExclusivelyLocked(PageId pid) {
		Map<TransactionId, Permissions> perms = mLocks.column(pid);
		if (perms.isEmpty()) return false;
		return perms.values().iterator().next() == Permissions.READ_WRITE;
	}

	private boolean isLocked(PageId pid) {
		Map<TransactionId, Permissions> perms = mLocks.column(pid);
		return !perms.isEmpty();
	}

	private void await(PageId pid) throws TransactionAbortedException {
		try {
			Condition guard = mPageGuards.get(pid);
			if (guard == null) {
				guard = mGuard.newCondition();
				mPageGuards.put(pid, guard);
			}
			guard.await();
		} catch (InterruptedException ie) {
			TransactionAbortedException tae = new TransactionAbortedException();
			tae.initCause(ie);
			throw tae;
		}
	}

	public void unlock(TransactionId tid, PageId pid) {
		mGuard.lock();
		try {
			unlockInternal(tid, pid);
		} finally {
			mGuard.unlock();
		}
	}

	public void unlockAll(TransactionId tid) {
		mGuard.lock();
		try {
			Map<PageId, Permissions> pagelocks = new HashMap<PageId, Permissions>();
			pagelocks.putAll(mLocks.row(tid));
			for (PageId pid : pagelocks.keySet())
				unlockInternal(tid, pid);
		} finally {
			mGuard.unlock();
		}
	}

	private void unlockInternal(TransactionId tid, PageId pid) {
		revokeLock(tid, pid);
		Condition guard = mPageGuards.get(pid);
		if (guard != null)
			guard.signalAll();
	}

	public boolean holdsLock(TransactionId tid, PageId pid) {
		mGuard.lock();
		try {
			return mLocks.get(tid, pid) != null;
		} finally {
			mGuard.unlock();
		}
	}
}
