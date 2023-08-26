package mydb;

import mydb.common.DbException;
import mydb.common.Type;
import mydb.storage.*;
import mydb.transaction.TransactionException;
import mydb.transaction.TransactionId;

import java.io.*;

public class MyDb {
    public static void main(String[] args)
            throws DbException, TransactionException {
        switch (args[0]) {
            case "convert" -> {
                // 用于将txt文件转化成二进制文件
                try {
                    if (args.length < 3 || args.length > 5) {
                        System.err.println("Unexpected number of arguments to convert");
                        return;
                    }
                    File sourceTxtFile = new File(args[1]);
                    File targetDatFile = new File(args[1].replaceAll(".txt", ".dat"));
                    int attributesNum = Integer.parseInt(args[2]); // 属性数量
                    Type[] types = new Type[attributesNum];
                    char fieldSeparator = ',';
                    if (args.length == 3) {
                        for (int i=0; i<attributesNum; i++) {
                            types[i] = Type.INT_TYPE;
                        }
                    } else {
                        String typeString = args[3];
                        String[] typeStrings = typeString.split(",");
                        if (typeStrings.length != attributesNum) {
                            System.err.println("The number of types does not agree with the number of columns");
                            return;
                        }
                        int index = 0;
                        for (String s: typeStrings) {
                            if (s.equalsIgnoreCase("int")) {
                                types[index++] = Type.INT_TYPE;
                            } else if (s.equalsIgnoreCase("string")) {
                                types[index++] = Type.STRING_TYPE;
                            } else {
                                System.err.println("Unknown type " + s);
                                return;
                            }
                        }
                        // 第5个参数是字段分隔符
                        if (args.length == 5) {
                            fieldSeparator = args[4].charAt(0);
                        }
                    }
                    HeapFileEncoder.convert(sourceTxtFile, targetDatFile,
                            BufferPool.getPageSize(), attributesNum, types, fieldSeparator);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            case "parser" -> {
                // SQL解析器
                String[] cmd = new String[args.length - 1];
                System.arraycopy(args, 1, cmd, 0 ,args.length - 1);

                try {
                    Class<?> c = Class.forName("mydb.Parser");
                    Class<?> s = String[].class;
                    java.lang.reflect.Method method = c.getMethod("main", s);
                    method.invoke(null, (Object) cmd);

                } catch (ClassNotFoundException e) {

                } catch (Exception e) {
                    System.out.println("Error in Parser");
                    e.printStackTrace();
                }
            }

            default -> {
                System.exit(1);
            }
        }
    }
}
