package java.mydb.storage;

import java.mydb.common.Database;
import java.mydb.common.DbException;
import java.mydb.common.Permissions;
import java.mydb.storage.lock.LockManager;
import java.mydb.storage.lock.PageLock;
import java.mydb.transaction.TransactionId;

import java.mydb.storage.evict.EvictStrategy;
import java.mydb.storage.evict.LRU;

import java.io.*;
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
        // TODO
        this.pagesNum = pagesNum;
        this.cache = new ConcurrentHashMap<>();
        this.evictStrategy = new LRU(pagesNum);
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
        throws DbException {
        // TODO
        int pageType = PageLock.SHARE;
        if (perm == Permissions.READ_WRITE) {
            pageType = PageLock.EXCLUSIVE;
        }
        long start = System.currentTimeMillis();
        long timeout = new Random().nextInt(2000) + 1000;
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
                // TODO 抛出一个事务异常
                throw new DbException("Transaction " + tid + " failed to get page " + pid);
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
     *
     * @param tid 事务ID
     * @param tableId 表ID
     * @param tuple 需要插入的元组
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple tuple)
        throws DbException, IOException {
        // TODO
    }

    /**
     *
     * @param tid 事务ID
     * @param tuple 需要删除的元组
     */
    public void deleteTuple(TransactionId tid, Tuple tuple)
        throws DbException, IOException {
        // TODO
    }


}
