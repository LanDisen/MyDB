package mydb.transaction;

import mydb.common.Database;

import java.io.IOException;

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
        try {
            // 事务开始记录到日志文件
            Database.getLogFile().logTransactionBegin(tid);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 事务结束后（commit/rollback）进行相关处理
     * @param abort true则为事务发生异常终止，false则该事务正常结束提交（commit）
     */
    public void end(boolean abort) throws IOException {
        if (started) {
            if (abort) {
                Database.getLogFile().logAbort(tid);
            }
            // 正常完成事务，释放事务锁
            Database.getBufferPool().transactionComplete(tid, !abort);
            if (!abort) {
                Database.getLogFile().logCommit(tid);
            }
            started = false; // 事务结束
        }
    }

    /**
     * 事务顺利完成，提交事务
     */
    public void commit() throws IOException {
        end(false);
    }

    /**
     * 事务异常（abort），进行事务回滚
     */
    public void rollback() throws IOException  {
        end(true);
    }
}
