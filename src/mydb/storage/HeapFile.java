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

    /**
     * 指定事务插入元组
     * @param tid 进行插入操作的事务ID
     * @param tuple 需要插入的元组
     * @return 返回修改了的页面列表
     */
    @Override
    public List<Page> insertTuple(TransactionId tid, Tuple tuple)
            throws DbException, IOException {
        List<Page> modifiedPages = new ArrayList<>(); // 由于插入了新的元组而将会被修改的页面列表
        int pagesNum = getPagesNum();
        for (int i=0; i<pagesNum; i++) {
            HeapPage page = (HeapPage) bufferPool.getPage(
                    tid, new HeapPageId(this.getId(), i), Permissions.READ_WRITE
            );
            // 页面没有空槽
            if (page.getEmptySlotsNum() == 0) {
                // 页面已满，该事务寻找下一个可以插入元组的页面
                // 页面已满需要释放该事务在页面上的锁，允许其它事务获取该页面，避免死锁
                bufferPool.releaseLock(tid, page.getId());
                continue;
            }
            page.insertTuple(tuple);
            modifiedPages.add(page);
            return modifiedPages;
        }
        // 缓冲池中的页面都满了，需要创建新的页面并写入文件
        BufferedOutputStream outputStream = new BufferedOutputStream(
                new FileOutputStream(file, true));
        byte[] emptyPageData = HeapPage.createEmptyPageData();
        // 向文件末尾添加数据
        outputStream.write(emptyPageData);
        outputStream.close();
        // 将创建的空页面数据加载到cache中
        HeapPage page = (HeapPage) bufferPool.getPage(
                tid,
                new HeapPageId(this.getId(), getPagesNum() - 1),
                Permissions.READ_WRITE);
        page.insertTuple(tuple);
        modifiedPages.add(page);
        return modifiedPages;
    }

    /**
     * 指定事务删除元组
     * @param tid 事务ID
     * @param tuple 需要进行删除的元组
     * @return 返回修改了的页面列表
     */
    @Override
    public List<Page> deleteTuple(TransactionId tid, Tuple tuple)
            throws DbException, IOException {
        HeapPage page = (HeapPage) bufferPool.getPage(
                tid,
                tuple.getRecordId().getPageId(),
                Permissions.READ_WRITE);
        page.deleteTuple(tuple);
        ArrayList<Page> modifiedPages = new ArrayList<>();
        modifiedPages.add(page);
        return modifiedPages;
    }

    @Override
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }

    /**
     * HeapFileIterator为HeapFile的内部静态类
     */
    public static class HeapFileIterator implements DbFileIterator {

        private final HeapFile heapFile;
        private final TransactionId tid;
        private Iterator<Tuple> iterator;
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
            this.iterator = getPageTuples(pageIndex);
        }

        @Override
        public void close() throws DbException  {
            iterator = null;
        }

        @Override
        public boolean hasNext() throws DbException  {
            if (iterator == null) {
                return false;
            }
            while (iterator != null && !iterator.hasNext()) {
                if (pageIndex < heapFile.getPagesNum() - 1) {
                    pageIndex++;
                    iterator = getPageTuples(pageIndex);
                } else {
                    iterator = null;
                }
            }
            if (iterator == null) {
                return false;
            }
            return iterator.hasNext();
        }

        @Override
        public Tuple next() throws DbException ,NoSuchElementException {
            if (iterator == null || !iterator.hasNext()) {
                throw new NoSuchElementException();
            }
            return iterator.next();
        }

        @Override
        public void rewind() throws DbException {
            close();
            open();
        }
    }
}
