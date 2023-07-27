package mydb.common;

import java.io.Serial;
import java.lang.Exception;

/**
 * 用于抛出数据库相关的异常
 */
public class DbException extends Exception {

    @Serial
    private static final long serialVersionUID = 1L;

    public DbException(String s) {
        super(s);
    }
}
