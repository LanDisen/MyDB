package mydb.transaction;

import java.lang.Exception;
import java.io.Serial;

/**
 * 事务异常终止（abort）时会抛出该异常
 */
public class TransactionException extends Exception {

    @Serial
    private static final long serialVersionUID = 1L;

    public TransactionException() {}
}
