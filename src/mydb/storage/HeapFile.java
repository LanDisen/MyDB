package mydb.storage;

import mydb.common.Database;
import mydb.common.DbException;
import mydb.common.Permissions;
import mydb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile（堆文件）实现了DbFile接口，用于存储元组（Tuple）集合
 * 元组存放在页面中，每个页面都是固定大小（4096B）。HeapFile是HeapPage的集合
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc tupleDesc;
    private final BufferPool bufferPool;

    /**
     * HeapFile构造函数，存放再特定文件中
     * @param file 磁盘中的文件，用于存放HeapFile
     */
    public HeapFile(File file, TupleDesc tupleDesc) {
        this.file = file;
        this.tupleDesc = tupleDesc;
        this.bufferPool = Database.getBufferPool();
    }

    /**
     * @return 返回该HeapFile存放在磁盘中的文件
     */
    public File getFile() {
        return this.file;
    }

    /**
     * @return 返回该HeapFile唯一的ID标识
     */
    @Override
    public int getId() {
        return this.file.getAbsolutePath().hashCode();
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    @Override
    public Page readPage(PageId pid) {
        // 计算Page的偏移量
        int pageSize = BufferPool.getPageSize();
        int pageIndex = pid.getPageIndex();
        int offset = pageSize * pageIndex;
        Page page = null;
        // 用于访问文件数据，只有RandomAccessFile才有seek方法
        RandomAccessFile randomAccessFile = null;
        try {
            randomAccessFile = new RandomAccessFile(file, "r");
            byte[] data = new byte[pageSize];
            randomAccessFile.seek(offset);
            randomAccessFile.read(data);
            page = new HeapPage((HeapPageId) pid, data) ;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                assert randomAccessFile != null;
                randomAccessFile.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return page;
    }

    @Override
    public void writePage(Page page) throws IOException {
        int pageSize = BufferPool.getPageSize();
        int pageIndex = page.getId().getPageIndex();
        int offset = pageSize * pageIndex;
        RandomAccessFile randomAccessFile = null;
        try {
            randomAccessFile = new RandomAccessFile(file, "rw");
            randomAccessFile.seek(offset);
            randomAccessFile.write(page.getPageData());
        } finally {
            assert randomAccessFile != null;
            randomAccessFile.close();
        }
    }

    /**
     * @return 返回存放在该文件的Page数量
     */
    public int getPagesNum() {
        long fileLength = this.file.length();
        int pagesNum = (int) Math.ceil(fileLength * 1.0 / BufferPool.getPageSize());
        return pagesNum;
    }

    public List<Page> insertTuple(TransactionId tid, Tuple tuple) {
        // TODO
        return null;
    }

    public List<Page> deleteTuple(TransactionId tid, Tuple tuple) {
        // TODO
        return null;
    }


    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }

    /**
     * HeapFileIterator为HeapFile的内部静态类
     */
    public static class HeapFileIterator implements DbFileIterator {

        private final HeapFile heapFile;
        private final TransactionId tid;
        private Iterator<Tuple> tupleIterator;
        private int pageIndex;

        public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
            this.heapFile = heapFile;
            this.tid = tid;
        }

        private Iterator<Tuple> getPageTuples(int pageIndex) throws DbException {
            if (pageIndex < 0 || pageIndex >= heapFile.getPagesNum()) {
                String errorMsg = String.format("page %d do not exists in heap file %d", pageIndex, heapFile.getId());
                throw new DbException(errorMsg);
            }
            HeapPageId pid = new HeapPageId(heapFile.getId(), pageIndex);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            return page.iterator();
        }

        @Override
        public void open() throws DbException {
            this.pageIndex = 0;
            this.tupleIterator = getPageTuples(pageIndex);
        }

        @Override
        public void close() throws DbException  {
            tupleIterator = null;
        }

        @Override
        public boolean hasNext() throws DbException  {
            if (tupleIterator == null) {
                return false;
            }
            while (tupleIterator != null && !tupleIterator.hasNext()) {
                if (pageIndex < heapFile.getPagesNum() - 1) {
                    pageIndex++;
                    tupleIterator = getPageTuples(pageIndex);
                } else {
                    tupleIterator = null;
                }
            }
            if (tupleIterator == null) {
                return false;
            }
            return tupleIterator.hasNext();
        }

        @Override
        public Tuple next() throws DbException ,NoSuchElementException {
            if (tupleIterator == null || !tupleIterator.hasNext()) {
                throw new NoSuchElementException();
            }
            return tupleIterator.next();
        }
    }
}
