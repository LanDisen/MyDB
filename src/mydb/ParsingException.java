package mydb;

import java.io.Serial;

/**
 * �����쳣��ͨ���ڽ���ĳ���ַ���ʱ�׳����쳣
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
