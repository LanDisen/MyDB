package java.mydb.storage.lock;

import java.mydb.storage.PageId;
import java.mydb.transaction.TransactionId;

/**
 * 页面锁
 */
public class PageLock {

    // 锁的类型
    private int type;

    // 共享锁
    public static final int SHARE = 0;

    // 排他锁
    public static final int EXCLUSIVE = 0;

    // 事务ID
    private TransactionId transactionId;

    /**
     * 页面锁构造函数
     * @param type 锁的类型，包括共享锁（SHARE）和排他锁（EXCLUSIVE）
     * @param tid 事务ID
     */
    public PageLock(int type, TransactionId tid) {
        this.type = type;
        this.transactionId = tid;
    }

    public int getType() {
        return this.type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public TransactionId getTransactionId() {
        return transactionId;
    }

    @Override
    public String toString() {
        return "PageLock{" +
                "type=" + type +
                ", transactionId=" + transactionId +
                "}";
    }
}
