package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private File file;
    private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        try {
            RandomAccessFile in = new RandomAccessFile(file, "r");
            byte[] data = new byte[BufferPool.getPageSize()];
            in.seek(pid.pageNumber() * BufferPool.getPageSize());
            in.readFully(data);
            in.close();
            return new HeapPage((HeapPageId) pid, data);
        } catch (Exception e) {}

        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        RandomAccessFile out = new RandomAccessFile(file, "rw");
        PageId pid = page.getId();
        out.seek(pid.pageNumber() * BufferPool.getPageSize());
        out.write(page.getPageData());
        out.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (file.length() / BufferPool.getPageSize());
    }

    int cnt = 0;

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
                throws DbException, IOException, TransactionAbortedException {

        ++cnt;

        ArrayList<Page> dirtyPages = new ArrayList<>();
        int numpages = numPages();
        for (int i = 0; i < numpages; ++i) {
            HeapPageId pid = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);

            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                dirtyPages.add(page);
                return dirtyPages;
            }
        }

        HeapPageId pid = new HeapPageId(getId(), numpages);
        HeapPage page = new HeapPage(pid, HeapPage.createEmptyPageData());
        page.insertTuple(t);
        writePage(page);
        return dirtyPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t)
                throws DbException, TransactionAbortedException {
        ArrayList<Page> dirtyPages = new ArrayList<>();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        dirtyPages.add(page);
        return dirtyPages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new DbFileIterator() {
            private HeapFile file;
            private TransactionId transactionId;
            private boolean opened;
            private HeapPageId pageId;
            private Iterator<Tuple> iterator;

            {
                this.file = HeapFile.this;
                this.transactionId = tid;
                this.opened = false;
                this.pageId = new HeapPageId(getId(), 0);
                this.iterator = null;
            }

            @Override
            public void open() throws DbException, TransactionAbortedException {
                opened = true;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (!opened) {
                    return false;
                }
                while (iterator == null || !iterator.hasNext()) {
                    if (pageId.pageNumber() >= file.numPages()) {
                        return false;
                    }
                    HeapPage page = (HeapPage) Database.getBufferPool().getPage(transactionId, pageId, Permissions.READ_ONLY);
                    if (page == null) {
                        return false;
                    }
                    iterator = page.iterator();
                    pageId = new HeapPageId(pageId.getTableId(), pageId.pageNumber() + 1);
                }
                return true;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return iterator.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                pageId = new HeapPageId(pageId.getTableId(), 0);
                iterator = null;
            }

            @Override
            public void close() {
                opened = false;
            }
        };
    }

}

