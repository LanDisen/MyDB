package java.mydb.storage.lock;

import java.mydb.storage.Page;
import java.mydb.storage.PageId;

import java.mydb.transaction.TransactionId;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockManager���ڹ����������������������ͷ������鿴ָ��ҳ���ָ�������Ƿ�����
 */
public class LockManager {

    /**
     * һ�������Ӧһ����
     */
    private Map<PageId, Map<TransactionId, PageLock>> pageLocks;

    public LockManager() {
        pageLocks = new ConcurrentHashMap<PageId, Map<TransactionId, PageLock>>();
    }

    /**
     * ָ�������Զ�ָ��ҳ�����
     * @param pid ҳ��ID
     * @param tid ����ID
     * @param lockType �������ͣ�������������SHARE������������EXCLUSIVE��
     */
    public synchronized boolean lock(PageId pid, TransactionId tid, int lockType)
            throws InterruptedException{
        final String lockTypeStr = (lockType == 0 ? "read lock": "write lock");
        final String threadName = Thread.currentThread().getName();
        // ���pidҳ����ڵ������Լ���Ӧ����
        Map<TransactionId, PageLock> locks = pageLocks.get(pid);
        // ����ҳ��û���κ�������tid�����ܹ��ɹ�������
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
        // ������������ڸ�ҳ��������
        if (pageLock != null) {
            // �����������
            if (lockType == PageLock.SHARE) {
                System.out.println(threadName + ": page " + pid +
                        " have read lock with the same transaction " + tid +
                        "set " + lockTypeStr + " successfully");
                return true;
            }
            // ��������д��
            if (lockType == PageLock.EXCLUSIVE) {
                if (locks.size() > 1) {
                    // ��ҳ�������������1��������Ȼ�Ƕ�����������д��ʧ��
                    System.out.println(threadName + ": page " + pid +
                            " have many read locks, transaction " + tid +
                            " failed to set " + lockTypeStr);
                    // TODO �׳������쳣
                    return false;
                }
                if (locks.size() == 1) {
                    if (pageLock.getType() == PageLock.EXCLUSIVE) {
                        // �������ڸ�ҳ��������һ��д��������ɹ�
                        System.out.println(threadName + ": page " + pid +
                                " have write lock with the same transaction " + tid +
                                " set " + lockTypeStr + " successfully");
                        return true;
                    }
                    if (pageLock.getType() == PageLock.SHARE) {
                        // �������ڸ�ҳ���Ѿ���һ��������ֱ������Ϊд��������д���ɹ�
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
            // �������ڸ�ҳ������������Ҫ�ж����������ڸ�ҳ�����������
            if (lockType == PageLock.SHARE) {
                if (locks.size() > 1) {
                    // ��ҳ�����кܶࣨ��������1������������Ȼ���Ƕ�����
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
                    // ��Map��Ѱ��Ψһ����Ч��������pl
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
            // �������ڸ�ҳ��������д��
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

    // ��Ӧ�����ڶ�Ӧҳ�����ͷ���
    public synchronized void unlock(PageId pid, TransactionId tid) {
        // TODO ��ʵ��
    }

    // �ж������ڸ�ҳ�����Ƿ������
    public synchronized boolean hasLock(PageId pid, TransactionId tid) {
        // TODO ��ʵ��
        return false;
    }

    // ����������ͷŸ�����������ҳ���ϳ��е���
    public synchronized void unlockAll() {
        // TODO ��ʵ��

    }

}
