package simpledb;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = PAGE_SIZE;
    }

    private static class PageIdWithTime implements Comparable<PageIdWithTime> {
        public long t;
        public PageId pid;

        public PageIdWithTime(long t, PageId pid) {
            this.t = t;
            this.pid = pid;
        }

        public int compareTo(PageIdWithTime oth) {
            if (t != oth.t) {
                return (int) (t - oth.t);
            }
            return pid.hashCode() - oth.pid.hashCode();
        }
    }

    private static class LockTriple {
        TransactionId tid;
        PageId pid;
        Permissions perm;

        public LockTriple(TransactionId tid, PageId pid, Permissions perm) {
            this.tid = tid;
            this.pid = pid;
            this.perm = perm;
        }

        @Override
        public int hashCode() {
            return (tid.hashCode() * 313 + pid.hashCode()) * 97 + perm.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof LockTriple)) {
                return false;
            }
            return tid.equals(((LockTriple) obj).tid) && pid.equals(((LockTriple) obj).pid) && perm.equals(((LockTriple) obj).perm);
        }
    }

    private static class LockManager {
        enum PageStatus {
            IDLE, SINGLE_READ, MULTI_READ, SINGLE_WRITE
        }

        public Map<PageId, Map<TransactionId, Permissions>> pidToLock;
        public Map<TransactionId, Map<PageId, Permissions>> tidToLock;

        public LockManager() {
            pidToLock = new ConcurrentHashMap<>();
            tidToLock = new ConcurrentHashMap<>();
        }

        private synchronized PageStatus getPageStatus(PageId pid) {
            if (!pidToLock.containsKey(pid)) {
                return PageStatus.IDLE;
            }
            Map<TransactionId, Permissions> map = pidToLock.get(pid);
            if (map.size() == 1) {
                return map.values().contains(Permissions.READ_ONLY) ? PageStatus.SINGLE_READ : PageStatus.SINGLE_WRITE;
            } else if (map.size() > 1) {
                return PageStatus.MULTI_READ;
            } else {
                return PageStatus.IDLE;
            }
        }

        private synchronized void updateLock(TransactionId tid, PageId pid, Permissions perm) {
            if (!pidToLock.containsKey(pid)) {
                pidToLock.put(pid, new ConcurrentHashMap<>());
            }
            if (!tidToLock.containsKey(tid)) {
                tidToLock.put(tid, new ConcurrentHashMap<>());
            }

            pidToLock.get(pid).put(tid, perm);
            tidToLock.get(tid).put(pid, perm);
        }

        private synchronized boolean innerGetLock(TransactionId tid, PageId pid, Permissions perm) {
            PageStatus status = getPageStatus(pid);

            if (perm == Permissions.READ_ONLY) {
                if (status == PageStatus.IDLE || status == PageStatus.SINGLE_READ || status == PageStatus.MULTI_READ) {
                    updateLock(tid, pid, perm);
                    return true;
                } else { // SINGLE_WRITE
                    return pidToLock.get(pid).keySet().contains(tid);
                }
            } else { // READ_WRITE
                if (status == PageStatus.IDLE) {
                    updateLock(tid, pid, perm);
                    return true;
                } else if (status == PageStatus.SINGLE_WRITE) {
                    return pidToLock.get(pid).keySet().contains(tid);
                } else if (status == PageStatus.SINGLE_READ) {
                    if (pidToLock.get(pid).keySet().contains(tid)) {
                        updateLock(tid, pid, perm);
                        return true;
                    } else {
                        return false;
                    }
                } else { // MULTI_READ
                    return false;
                }
            }
        }

        public synchronized boolean getLock(TransactionId tid, PageId pid, Permissions perm, DeadlockChecker deadlockChecker) {
            if (innerGetLock(tid, pid, perm)) {
                deadlockChecker.removeRequest(tid, pid);
                return true;
            } else {
                return false;
            }
        }

        public synchronized void releaseLock(TransactionId tid, PageId pid) {
            if (pidToLock.containsKey(pid)) {
                pidToLock.get(pid).remove(tid);
            }
            if (tidToLock.containsKey(tid)) {
                tidToLock.get(tid).remove(pid);
            }
        }

        public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
            return pidToLock.containsKey(pid) && pidToLock.get(pid).containsKey(tid);
        }
    }

    private static class DeadlockChecker {
        private Map<TransactionId, Set<PageId>> pageRequests;

        public DeadlockChecker(LockManager lockManager) {
            pageRequests = new ConcurrentHashMap<>();
        }

        public synchronized void addRequest(TransactionId tid, PageId pid) {
            if (!pageRequests.containsKey(tid)) {
                pageRequests.put(tid, new HashSet<>());
            }
            pageRequests.get(tid).add(pid);
        }

        public synchronized void removeRequest(TransactionId tid, PageId pid) {
            if (pageRequests.containsKey(tid)) {
                pageRequests.get(tid).remove(pid);
            }
        }

        public synchronized boolean checkDeadlock(TransactionId tid, PageId pid, LockManager lockManager) {
            Set<TransactionId> visitedTid = new HashSet<>();
            Set<PageId> visitedPid = new HashSet<>();

            Queue<Object> queue = new LinkedList<>();
            queue.offer(pid);
            while (!queue.isEmpty()) {
                Object first = queue.poll();
                if (first instanceof TransactionId) {
                    TransactionId now = (TransactionId) first;
                    if (pageRequests.containsKey(now)) {
                        for (PageId to : pageRequests.get(now)) {
                            if (!visitedPid.contains(to)) {
                                queue.offer(to);
                                visitedPid.add(to);
                            }
                        }
                    }
                } else { // first instanceof PageId
                    PageId now = (PageId) first;
                    if (lockManager.pidToLock.containsKey(now)) {
                        for (TransactionId to : lockManager.pidToLock.get(now).keySet()) {
                            if (to.equals(tid)) {
                                return true;
                            }
                            if (!visitedTid.contains(to)) {
                                queue.offer(to);
                                visitedTid.add(to);
                            }
                        }
                    }
                }
            }

            return false;
        }
    }

    private Map<PageId, Page> pageBuffer;
    private Set<PageIdWithTime> pageIdWithTimes;
    private Map<PageId, Long> lastOptTime;
    private int capacity;

    private final LockManager lockManager;
    private final DeadlockChecker deadlockChecker;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        pageBuffer = new ConcurrentHashMap<>();
        pageIdWithTimes = Collections.synchronizedSet(new TreeSet<>());
        lastOptTime = new ConcurrentHashMap<>();
        capacity = numPages;

        lockManager = new LockManager();
        deadlockChecker = new DeadlockChecker(lockManager);
    }

    /**
     * Update the lastOptTime for given page ID.
     *
     * @param pid the ID of the requested page
     * @param t the new lastOptTime (in millisecond)
     */
    private synchronized void updateLastOptTime(PageId pid, long t) {
        if (lastOptTime.containsKey(pid)) {
            long oldT = lastOptTime.get(pid);
            pageIdWithTimes.remove(new PageIdWithTime(oldT, pid));
        }

        lastOptTime.replace(pid, t);
        pageIdWithTimes.add(new PageIdWithTime(t, pid));
    }

    /**
     * Update the lastOptTime for given page ID to current time.
     *
     * @param pid the ID of the requested page
     */
    private synchronized void updateLastOptTime(PageId pid) {
        updateLastOptTime(pid, Calendar.getInstance().getTimeInMillis());
    }

    /**
     * Add a page to buffer pool at time t.
     * @param page instance of the added page
     * @param t the time (in millisecond) the adding operation occurs
     */
    private synchronized void addPage(Page page, long t) throws DbException {
        PageId pid = page.getId();

        if (pageBuffer.containsKey(pid)) {
            pageBuffer.replace(pid, page);
            updateLastOptTime(pid, t);
            return;
        }

        if (pageBuffer.size() == capacity) {
            evictPage();
        }

        pageBuffer.put(pid, page);
        pageIdWithTimes.add(new PageIdWithTime(t, pid));
        lastOptTime.put(pid, t);
    }

    /**
     * Add a page to buffer pool.
     * @param page instance of the added page
     */
    private synchronized void addPage(Page page) throws DbException {
        addPage(page, Calendar.getInstance().getTimeInMillis());
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
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
                throws TransactionAbortedException, DbException {
        while (!lockManager.getLock(tid, pid, perm, deadlockChecker)) { // If successfully get a lock, simultaneously remove it in the deadlockChecker
            synchronized (lockManager) {
                if (deadlockChecker.checkDeadlock(tid, pid, lockManager)) {
                    throw new TransactionAbortedException();
                }
                deadlockChecker.addRequest(tid, pid);
            }
        }

        Page page = pageBuffer.get(pid);

        if (page != null) {
            updateLastOptTime(pid);
            return page;
        }

        Page newPage = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);

        addPage(newPage);

        return newPage;
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
    public void releasePage(TransactionId tid, PageId pid) {
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        return lockManager.holdsLock(tid, pid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
                throws IOException {
        if (commit) {
            // And this one as well
            flushPages(tid);
        } else if (lockManager.tidToLock.containsKey(tid)) {
            for (Map.Entry<PageId, Permissions> entry : lockManager.tidToLock.get(tid).entrySet()) {
                PageId pid = entry.getKey();

                // This fuck is to pass the unit test
                if (!pageBuffer.containsKey(pid)) {
                    continue;
                }

                if (entry.getValue() == Permissions.READ_WRITE) {
                    long t = lastOptTime.get(pid);
                    pageBuffer.remove(pid);
                    pageIdWithTimes.remove(new PageIdWithTime(t, pid));
                    lastOptTime.remove(pid);
                }
            }
        }

        if (lockManager.tidToLock.containsKey(tid)) {
            LinkedList<PageId> pidList = new LinkedList<>(lockManager.tidToLock.get(tid).keySet());
            for (PageId pid : pidList) {
                lockManager.releaseLock(tid, pid);
            }
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
                throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> pagesModified = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
        for (Page page : pagesModified) {
            page.markDirty(true, tid);
            addPage(page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
                throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> pagesModified = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId()).deleteTuple(tid, t);
        for (Page page : pagesModified) {
            page.markDirty(true, tid);
            addPage(page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId pid : pageBuffer.keySet()) {
            flushPage(pid);
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     *  Needed by the recovery manager to ensure that the
     *  buffer pool doesn't keep a rolled back page in its
     *  cache.
     *
     *  Also used by B+ tree files to ensure that deleted pages
     *  are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        Page page = pageBuffer.get(pid);
        if (page != null) {
            try {
                flushPage(pid);
            } catch (IOException e) {
                e.printStackTrace();
            }

            long t = lastOptTime.get(pid);
            pageBuffer.remove(pid);
            pageIdWithTimes.remove(new PageIdWithTime(t, pid));
            lastOptTime.remove(pid);
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        Page page = pageBuffer.get(pid);
        if (page != null && page.isDirty() != null) {
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
            page.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        if (lockManager.tidToLock.containsKey(tid)) {
            for (PageId pid : lockManager.tidToLock.get(tid).keySet()) {
                flushPage(pid);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        if (pageBuffer.isEmpty()) {
            throw new DbException("Unable to evict page: Buffer pool is empty. ");
        }

        for (PageIdWithTime piwt : pageIdWithTimes) {
            Page page = pageBuffer.get(piwt.pid);
            if (page.isDirty() == null) {
                discardPage(piwt.pid);
                return;
            }
        }

        throw new DbException("Unable to evict page: All pages are dirty. ");
    }
}
