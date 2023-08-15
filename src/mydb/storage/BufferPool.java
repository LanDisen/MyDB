package mydb.storage;

import mydb.common.Database;
import mydb.common.DbException;
import mydb.common.Permissions;
import mydb.storage.lock.LockManager;
import mydb.storage.lock.PageLock;
import mydb.transaction.Transaction;
import mydb.transaction.TransactionException;
import mydb.transaction.TransactionId;

import mydb.storage.evict.EvictStrategy;
import mydb.storage.evict.LRU;

import java.io.*;
import java.util.*;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 缓冲池用于管理页面（Page）读写
 * 缓冲池也负责锁的功能，当一个事务需要获取页面时，缓冲池需要检查锁
 */
public class BufferPool {

    /**
     * 缓冲池的默认页面大小（字节数）
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    /**
     * 缓冲池实际使用的页面大小，默认为4096bytes
     */
    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * 缓冲池管理缓存的默认最大页面数量
     */
    public static final int DEFAULT_PAGES_NUM = 50;

    /**
     * 该缓冲池实际管理缓存的最大页面数量
     */
    private int pagesNum = DEFAULT_PAGES_NUM;

    /**
     * cache缓存该BufferPool管理维护的页面
     */
    private Map<PageId, Page> cache;

    /**
     * 页面驱逐策略使用LRU（less recent used）
     */
    private EvictStrategy evictStrategy;

    /**
     * 管理缓冲池中页面的锁
     */
    private LockManager lockManager;

    /**
     * 创建缓冲池，最大可以缓存pagesNum数量的页面（Pages）
     * @param pagesNum 该缓冲池可以缓存的最大页面数量
     */
    public BufferPool(int pagesNum) {
        this.pagesNum = pagesNum;
        this.cache = new ConcurrentHashMap<>();
        this.evictStrategy = new LRU(pagesNum);
        this.lockManager = new LockManager();
    }

    public static int getPageSize() {
        return pageSize;
    }

    /**
     * 根据事务ID和页面ID获得具体页面信息，需要有相关权限（Permissions）
     * 多个事务对同一个页面操作时可能需要上锁，阻塞其它事务
     * 当在缓冲池查询到对应页面时直接返回，未查询到则将所需页面加入到缓冲池再返回。
     * 如果缓冲池空间不足则需要页面交换，选择一个页面驱逐并添加新的页面
     * @param tid 事务ID
     * @param pid 页面ID
     * @param perm Permissions，在该页面的操作权限，包括READ_ONLY和READ_WRITE
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws DbException, TransactionException {
        int pageType = PageLock.SHARE;
        if (perm == Permissions.READ_WRITE) {
            pageType = PageLock.EXCLUSIVE;
        }
        long start = System.currentTimeMillis(); // 开始时间
        long timeout = new Random().nextInt(2000) + 1000; // 超时时间
        while (true) {
            try {
                if (lockManager.lock(pid, tid, pageType)) {
                    // 事务对该页面请求锁成功
                    break;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            long now = System.currentTimeMillis();
            if (now - start > timeout) {
                // 请求页面超时
                throw new TransactionException();
            }
        }
        // 此时事务成功获取了一个页面
        if (!cache.containsKey(pid)) {
            // 缓冲池中不存在该页面，添加新页面
            DbFile dbFile = Database.getCatalog().getDbFile(pid.getTableId());
            Page page = dbFile.readPage(pid);
            evictStrategy.modifyData(pid);
            if (cache.size() == pagesNum) {
                // 缓冲池存放页面已满，需对其中一个页面进行驱逐
                PageId evictPageId = evictStrategy.getEvictPageId();
                cache.remove(evictPageId);
            }
            cache.put(pid, page);
        }
        return cache.get(pid);
    }

    /**
     * 指定事务释放在指定页面的锁
     * @param tid 事务ID
     * @param pid 需要释放锁的页面ID
     */
    public void releasePage(TransactionId tid, PageId pid) {
        lockManager.unlock(pid, tid);
    }

    /**
     * 事务完成，释放该事务持有的全部锁
     * @param tid 事务ID
     */
    public void transactionComplete(TransactionId tid) {
        // TOD        transactionComplete(tid, true);
    }

    /**
     * 完成事务进行提交（commit）或者回滚（rollback）
     * @param tid 事务ID
     * @param commit true则事务顺利完成进行提交，false为事务异常终止（abort）
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        if (commit) {
            // 提交事务，刷新该事务持有的页面写入到磁盘（使之不dirty）
            try {
                flushPages(tid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            // 终止该事务，从磁盘重新加载脏页（恢复页面刷新前的状态）
            recoverPages(tid);
        }
        // 事务完成，释放该事务持有的所有锁
        lockManager.unlockAll(tid);
    }

    /**
     * 事务异常终止，需要回滚操作，从磁盘中重新加载脏页
     * @param tid 事务ID
     */
    private synchronized void recoverPages(TransactionId tid) {
        for (Map.Entry<PageId, Page> entry: cache.entrySet()) {
            PageId pid = entry.getKey();
            Page page = entry.getValue();
            if (page.isDirty().equals(tid)) {
                int tableId = pid.getTableId();
                DbFile dbFile = Database.getCatalog().getDbFile(tableId);
                Page dirtyPage = dbFile.readPage(pid);
                cache.put(pid, dirtyPage);
            }
        }
    }

    /**
     * 判断事务在指定页面上是否持有锁
     * @param tid 事务ID
     * @param pid 页面ID
     * @return 如果指定事务在指定页面上持有锁则返回true，否则返回false
     */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        return lockManager.hasLock(pid, tid);
    }

    /**
     * 更新缓冲池中需要进行更新的页面列表
     * @param pages 需要更新的页面列表
     * @param tid 对页面进行操作的事务ID
     */
    private void updatePages(List<Page> pages, TransactionId tid) throws DbException {
        for (Page page: pages) {
            page.setDirty(true, tid);
            if (cache.size() == pagesNum) {
                // 缓冲池存放页面数量已满，驱逐页面
                evictPage();
            }
            cache.put(page.getId(), page);
        }
    }

    /**
     * 指定事务在指定的表中插入一个新元组，需要将页面设置为dirty
     * @param tid 事务ID
     * @param tableId 表ID
     * @param tuple 需要插入的元组
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple tuple)
        throws DbException, IOException, TransactionException {
        // 获取需要插入元组的表
        DbFile dbFile = Database.getCatalog().getDbFile(tableId);
        updatePages(dbFile.insertTuple(tid, tuple), tid);
    }

    /**
     * 指定事务在指定的表中删除元组，并将页面设置为dirty
     * @param tid 事务ID
     * @param tuple 需要删除的元组
     */
    public void deleteTuple(TransactionId tid, Tuple tuple)
        throws DbException, IOException, TransactionException {
        DbFile dbFile = Database.getCatalog().getDbFile(
                tuple.getRecordId().getPageId().getTableId());
        updatePages(dbFile.deleteTuple(tid, tuple), tid);
    }

    /**
     * 刷新磁盘中的一个指定页面，将其写入磁盘，使之不dirty
     * @param pid 页面ID
     */
    public synchronized void flushPage(PageId pid) throws IOException {
        Page flushPage = cache.get(pid); // 获得需要刷新的页面
        // 通过tableId得到对应的DbFIle，将page写入对应的DbFile中
        int tableId = pid.getTableId();
        DbFile dbFile = Database.getCatalog().getDbFile(tableId);
        // 获得上一个对该页面操作的事务ID
        TransactionId tid = flushPage.isDirty();
        if (tid != null) {
            // TODO 日志处理
        }
        dbFile.writePage(flushPage);
        flushPage.setDirty(false, null);
    }

    /**
     * 刷新磁盘中所有的脏页，将它们写入到磁盘，使这些页面不dirty
     */
    public synchronized void flushAllPages() throws IOException {
        for (Map.Entry<PageId, Page> entry: cache.entrySet()) {
            Page page = entry.getValue();
            if (page.isDirty() != null) {
                // 页面dirty
                flushPage(page.getId());
            }
        }
    }

    /**
     * 刷新指定事务对应的所有页面
     * @param tid 事务ID
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        for (Map.Entry<PageId, Page> entry: cache.entrySet()) {
            Page page = entry.getValue();
            // TODO 数据库恢复相关操作待补充
            if (page.isDirty() == tid) {
                // 只刷新指定事务的相应页面
                flushPage(page.getId());
            }
        }
    }

    /**
     * 从缓冲池中删除一个指定页面
     * // TODO 后续需要实现数据库恢复的内容，如保存回滚（ROLL BACK）页面
     * @param pid 页面ID
     */
    public synchronized void discardPage(PageId pid) {
        cache.remove(pid);
    }

    /**
     * 从缓冲池中驱逐一个页面，需要刷新磁盘中的页面以确保脏页已更新
     */
    private synchronized void evictPage() throws DbException {
        PageId evictPageId = null;
        Page page = null;
        boolean isAllDirty = true;
        for (int i=0; i<cache.size(); i++) {
            evictPageId = evictStrategy.getEvictPageId();
            page = cache.get(evictPageId);
            if (page.isDirty() != null) {
                evictStrategy.modifyData(evictPageId);
            } else {
                // 有不dirty的页面，可以进行驱逐
                isAllDirty = false;
                discardPage(evictPageId); // 从cache中删除该页面
                break;
            }
        }
        if (isAllDirty) {
            // 所有页面都dirty
            // TODO 随机选择一个页面进行驱逐
            throw new DbException("All the pages are dirty.");
        }
    }
}
