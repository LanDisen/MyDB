package mydb.transaction;

/**
 * 事务（Transaction）类
 * 事务的四大特性（ACID）：原子性、一致性、隔离性、持久性
 */
public class Transaction {

    private final TransactionId tid;

    volatile boolean started = false;

    public Transaction() {
        tid = new TransactionId();
    }

    public TransactionId getId() {
        return tid;
    }

    /**
     * 开始执行该事务
     */
    public void start() {
        started = true;
        // TODO 异常捕获
    }

    /**
     * 处理事务结束后（commit/rollback）的细节
     */
    public void over(boolean abort) {
        if (started) {
            if (abort) {
                // TODO ROLLBACK
            } else {
                // TODO COMMIT
            }
        }
        // 事务执行结束
        started = false;
    }

//    public void commit();
//
//    public rollback();

}
