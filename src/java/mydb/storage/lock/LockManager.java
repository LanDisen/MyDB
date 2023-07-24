package java.mydb.storage.lock;

import java.mydb.storage.Page;
import java.mydb.storage.PageId;

import java.mydb.transaction.TransactionId;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockManager用于管理锁，包括：请求锁、释放锁、查看指定页面的指定事务是否有锁
 */
public class LockManager {

    /**
     * 一个事务对应一个锁
     */
    private Map<PageId, Map<TransactionId, PageLock>> pageLocks;

    public LockManager() {
        pageLocks = new ConcurrentHashMap<PageId, Map<TransactionId, PageLock>>();
    }

    /**
     * 指定事务尝试对指定页面加锁
     * @param pid 页面ID
     * @param tid 事务ID
     * @param lockType 锁的类型，包括共享锁（SHARE）和排他锁（EXCLUSIVE）
     */
    public synchronized boolean lock(PageId pid, TransactionId tid, int lockType)
            throws InterruptedException{
        final String lockTypeStr = (lockType == 0 ? "read lock": "write lock");
        final String threadName = Thread.currentThread().getName();
        // 获得pid页面存在的事务以及对应的锁
        Map<TransactionId, PageLock> locks = pageLocks.get(pid);
        // 若该页面没有任何锁，则tid事务能够成功请求锁
        if (locks == null || locks.size() == 0) {
            PageLock pageLock = new PageLock(lockType, tid);
            locks = new ConcurrentHashMap<>();
            locks.put(tid, pageLock);
            pageLocks.put(pid, locks);
            System.out.println(threadName + ": page " + pid + " have no lock, " +
                    "transaction " + tid + " set " + lockTypeStr + " successfully");
            return true;
        }
        PageLock pageLock = locks.get(tid);
        // 如果该事务在在该页面上有锁
        if (pageLock != null) {
            // 尝试请求读锁
            if (lockType == PageLock.SHARE) {
                System.out.println(threadName + ": page " + pid +
                        " have read lock with the same transaction " + tid +
                        "set " + lockTypeStr + " successfully");
                return true;
            }
            // 尝试请求写锁
            if (lockType == PageLock.EXCLUSIVE) {
                if (locks.size() > 1) {
                    // 该页面具有数量大于1的锁（显然是读锁），请求写锁失败
                    System.out.println(threadName + ": page " + pid +
                            " have many read locks, transaction " + tid +
                            " failed to set " + lockTypeStr);
                    // TODO 抛出事务异常
                    return false;
                }
                if (locks.size() == 1) {
                    if (pageLock.getType() == PageLock.EXCLUSIVE) {
                        // 该事务在该页面上已有一个写锁，请求成功
                        System.out.println(threadName + ": page " + pid +
                                " have write lock with the same transaction " + tid +
                                " set " + lockTypeStr + " successfully");
                        return true;
                    }
                    if (pageLock.getType() == PageLock.SHARE) {
                        // 该事务在该页面已经有一个读锁，直接设置为写锁，请求写锁成功
                        pageLock.setType(PageLock.EXCLUSIVE);
                        locks.put(tid, pageLock);
                        pageLocks.put(pid, locks);
                        System.out.println(threadName + ": page " + pid +
                                " have one read lock with the same transaction " + tid +
                                " set " + lockTypeStr + "successfully");
                        return true;
                    }
                }
            }
        } else { // equals to: if (pageLock == null)
            // 该事务在该页面上无锁，需要判断其它事务在该页面的锁的类型
            if (lockType == PageLock.SHARE) {
                if (locks.size() > 1) {
                    // 该页面上有很多（数量大于1）事务锁（显然都是读锁）
                    pageLock = new PageLock(lockType, tid);
                    locks.put(tid, pageLock);
                    pageLocks.put(pid, locks);
                    System.out.println(threadName + ": page " + pid +
                            " have no locks, transaction " + tid +
                            " set " + lockTypeStr + " successfully");
                    return true;
                }
                PageLock tmpLock = null;
                for (PageLock pl: locks.values()) {
                    // 在Map中寻找唯一的有效的事务锁pl
                    tmpLock = pl;
                }
                if (locks.size() == 1) {
                    if (tmpLock.getType() == PageLock.SHARE) {
                        pageLock = new PageLock(lockType, tid);
                        locks.put(tid, pageLock);
                        pageLocks.put(pid, locks);
                        System.out.println(threadName + ": page " + pid +
                                " have no locks, transaction " + tid +
                                " set " + lockTypeStr + " successfully");
                        return true;
                    }
                    if (tmpLock.getType() == PageLock.EXCLUSIVE) {
                        wait(50);
                        return false;
                    }
                }
            }
            // 该事务在该页面上请求写锁
            if (lockType == PageLock.EXCLUSIVE) {
//                wait(10);
//                return false;
                if (locks.size() > 0) {
                    wait(10);
                    return false;
                } else {
                    // locks.size() == 0
                    pageLock = new PageLock(lockType, tid);
                    locks.put(tid, pageLock);
                    pageLocks.put(pid, locks);
                    return true;
                }
            }
        }
       return false;
    }

    // 对应事务在对应页面上释放锁
    public synchronized void unlock(PageId pid, TransactionId tid) {
        // TODO 待实现
    }

    // 判断事务在该页面上是否持有锁
    public synchronized boolean hasLock(PageId pid, TransactionId tid) {
        // TODO 待实现
        return false;
    }

    // 事务结束，释放该事务在所有页面上持有的锁
    public synchronized void unlockAll() {
        // TODO 待实现

    }

}
