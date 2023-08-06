package mydb;

import java.io.Serial;

/**
 * 解析异常，通常在解析某个字符串时抛出该异常
 */
public class ParsingException extends Exception {

    @Serial
    private static final long serialVersionUID = 1L;

    public ParsingException(String str) {
        super(str);
    }

    public ParsingException(Exception e) {
        super(e);
    }
}
