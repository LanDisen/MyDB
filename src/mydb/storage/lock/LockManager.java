package mydb.storage.lock;

import mydb.storage.PageId;

import mydb.transaction.TransactionId;
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
        // ���pidҳ����ڵ������Լ���Ӧ����
        Map<TransactionId, PageLock> locks = pageLocks.get(pid);
        // ����ҳ��û���κ�������tid�����ܹ��ɹ�������
        if (locks == null || locks.size() == 0) {
            PageLock pageLock = new PageLock(lockType, tid);
            locks = new ConcurrentHashMap<>();
            locks.put(tid, pageLock);
            pageLocks.put(pid, locks);
            return true;
        }
        PageLock pageLock = locks.get(tid);
        // ������������ڸ�ҳ��������
        if (pageLock != null) {
            // �����������
            if (lockType == PageLock.SHARE) {
                return true;
            }
            // ��������д��
            if (lockType == PageLock.EXCLUSIVE) {
                if (locks.size() > 1) {
                    // ��ҳ�������������1��������Ȼ�Ƕ�����������д��ʧ��
                    // TODO �׳������쳣
                    return false;
                }
                if (locks.size() == 1) {
                    if (pageLock.getType() == PageLock.EXCLUSIVE) {
                        // �������ڸ�ҳ��������һ��д��������ɹ�
                        return true;
                    }
                    if (pageLock.getType() == PageLock.SHARE) {
                        // �������ڸ�ҳ���Ѿ���һ��������ֱ������Ϊд��������д���ɹ�
                        pageLock.setType(PageLock.EXCLUSIVE);
                        locks.put(tid, pageLock);
                        pageLocks.put(pid, locks);
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


    /**
     * ��Ӧ�����ڶ�Ӧҳ�����ͷ���
     * @param pid ҳ��ID
     * @param tid ����ID
     */
    public synchronized void unlock(PageId pid, TransactionId tid) {
        Map<TransactionId, PageLock> locks = pageLocks.get(pid);
        if (locks == null || tid == null) {
            // ��ҳ������������δָ��tid��ֱ�ӷ���
            return;
        }
        PageLock lock = locks.get(tid);
        if (lock == null) {
            // �������ڸ�ҳ����û������ֱ�ӷ���
            return;
        }
        locks.remove(tid); // �������ͷ���
        if (locks.size() == 0) {
            // ��ҳ��û���κ���
            pageLocks.remove(pid);
        }
        this.notifyAll(); // ���������߳�
    }

    /**
     * �ж������ڸ�ҳ�����Ƿ������
     * @param pid ҳ��ID
     * @param tid ����ID
     */
    public synchronized boolean hasLock(PageId pid, TransactionId tid) {
        Map<TransactionId, PageLock> locks = pageLocks.get(pid);
        if (locks == null) {
            return false;
        }
        return (locks.get(tid) != null);
    }

    /**
     * ����������ͷŸ�����������ҳ���ϳ��е���
     */
    public synchronized void unlockAll(TransactionId tid) {
        Set<PageId> pageIdSet = pageLocks.keySet();
        for (PageId pid: pageIdSet) {
            unlock(pid, tid);
        }
    }
}
