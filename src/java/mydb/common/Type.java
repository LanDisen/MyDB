package java.mydb.common;

import java.mydb.storage.Field;
import java.mydb.storage.IntField;
import java.mydb.storage.StringField;

import java.text.ParseException;
import java.io.*;

/**
 * Type枚举类，用于表示MyDB中的数据类型
 * Type类的实例是静态对象，构造函数是private定义
 */
public enum Type implements Serializable {

    INT_TYPE() {

        @Override
        public int getLen() {
            return 4;
        }

        @Override
        public Field parse(DataInputStream dis) throws ParseException {
            try {
                return new IntField(dis.readInt());
            }  catch (IOException e) {
                throw new ParseException("couldn't parse", 0);
            }
        }

        @Override
        public String toString() {
            return "int";
        }
    },
    STRING_TYPE() {
        @Override
        public int getLen() {
            return STRING_LEN + 4;
        }

        public Field parse(DataInputStream dis) throws ParseException {
            try {
                int strLen = dis.readInt();
                byte[] bytes = new byte[strLen];
                int ret = dis.read(bytes);
                dis.skipBytes(STRING_LEN - strLen);
                return new StringField(new String(bytes), STRING_LEN);
            } catch (IOException e) {
                throw new ParseException("couldn't parse", 0);
            }
        }

        @Override
        public String toString() {
            return "str";
        }
    };

    public static final int STRING_LEN = 128;

    /**
     * @return 返回存储对应类型字段的字节数
     */
    public abstract int getLen();

    /**
     * @param dis 读取的输入柳DataInputStream
     * @return 返回与该对象类型相同的从dis中读取的Field对象
     */
    public abstract Field parse(DataInputStream dis) throws ParseException;

}
