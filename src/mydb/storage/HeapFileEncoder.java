package mydb.storage;

import mydb.common.Type;
import mydb.common.Util;

import java.io.*;
import java.util.Arrays;
import java.util.List;

/**
 * HeapFileEncoder用于读取类csv文件（逗号分隔）并将其转换为HeapPage
 */
public class HeapFileEncoder {

    /**
     * 用于将指定的tuples（只有int fields）转换为二进制的页面文件（PageFile）
     * @param tuples 元组集合，元组的每个字段都是int
     * @param outFile 需要写入的输出文件
     * @param pageBytesNum 输出文件中每个页面的字节数
     * @param fieldsNum 一个元组（tuple）的字段（field）数
     */
    public static void convert(List<List<Integer>> tuples, File outFile, int pageBytesNum, int fieldsNum)
            throws IOException {
        File tempInputFile = File.createTempFile("tempTable", ".txt");
        tempInputFile.deleteOnExit(); // 退出就删除文件
        BufferedWriter bw = new BufferedWriter(new FileWriter(tempInputFile));
        for (List<Integer> tuple: tuples) {
            int writtenFieldsNum = 0; // 已写入的字段数量
            for (Integer field: tuple) {
                writtenFieldsNum++;
                if (writtenFieldsNum > fieldsNum) {
                    throw new RuntimeException("Tuple has more than " + fieldsNum + " fields");
                }
                bw.write(String.valueOf(field));
                if (writtenFieldsNum < fieldsNum) {
                    bw.write(',');
                }
            }
            bw.write('\n');
        }
        bw.close();
        convert(tempInputFile, outFile, pageBytesNum, fieldsNum);
    }

    public static void convert(File inFile, File outFile, int pageBytesNum, int fieldsNum)
            throws IOException  {
        Type[] types = new Type[fieldsNum];
        Arrays.fill(types, Type.INT_TYPE);
        convert(inFile, outFile, pageBytesNum, fieldsNum, types);
    }

    public static void convert(File inFile, File outFile, int pageBytesNum, int fieldsNum, Type[] types)
            throws IOException {
        convert(inFile, outFile, pageBytesNum, fieldsNum, types, ',');
    }

    /**
     * 用于将文本输入文件（inFile）转换为二进制输出文件（outFile）<br>
     * 输入文件的组织格式为（每一行表示一个元组）：<br>
     * int,...,int\n<br>
     * int,...,int\n
     * @param inFile 文本输入文件，读取数据
     * @param outFile 二进制输出文件，写入数据
     * @param pageBytesNum 输出文件中每个页面的字节数
     * @param fieldsNum 文件中每一行的元组的字段数量
     * @param types 字段类型数组
     * @param fieldSeparator 文件中用于分隔属性的符号，如逗号
     * @throws IOException 输入或输出文件未打开（open）会抛出异常
     */
    public static void convert(File inFile, File outFile, int pageBytesNum,
                               int fieldsNum, Type[] types, char fieldSeparator)
            throws IOException {
        int recordBytesNum = 0; // 一条记录（元组）的字节数，对每个字段大小求和得到
        for (int i=0; i<fieldsNum; i++) {
            recordBytesNum += types[i].getLen();
        }
        int recordNum = (pageBytesNum * 8) / (recordBytesNum * 8 + 1); // floor
        // 页面header本质为bitmap，每个记录用1位表示
        int headerBytesNum = (int) Math.ceil(recordNum * 1.0 / 8);
        int headerBitsNum = headerBytesNum * 8;

        BufferedReader br = new BufferedReader(new FileReader(inFile));
        FileOutputStream os = new FileOutputStream(outFile);

        char[] buf = new char[1024];
        int curPos = 0;
        int recordCnt = 0;
        int pageNum = 0;
        int fieldIndex = 0;

        ByteArrayOutputStream headerBAOS = new ByteArrayOutputStream(headerBytesNum);
        DataOutputStream headerDOS = new DataOutputStream(headerBAOS);
        ByteArrayOutputStream pageBAOS = new ByteArrayOutputStream(pageBytesNum);
        DataOutputStream pageDOS = new DataOutputStream(pageBAOS);

        boolean done = false;
        boolean first = true;

        while (!done) {
            int c = br.read();
            if (c == '\r')
                continue;
            if (c == '\n') {
                if (first) {
                    continue;
                }
                recordCnt++;
                first = true;
            } else {
                first = false;
            }
            if (c == fieldSeparator || c == '\n' || c == '\r') {
                String str = new String(buf, 0, curPos);
                if (types[fieldIndex] == Type.INT_TYPE) {
                    try {
                        pageDOS.writeInt(Integer.parseInt(str.trim())); // str.trim() 去除首尾空格
                    } catch (NumberFormatException e) {
                        System.out.println("wrong line: " + str);
                    }
                } else if (types[fieldIndex] == Type.STRING_TYPE) {
                    str = str.trim(); // 去除首尾空格
                    int overflow = Type.STRING_LEN - str.length();
                    if (overflow < 0) {
                        // 字符串字段最大长度为128
                        str = str.substring(0, Type.STRING_LEN);
                    }
                    pageDOS.writeInt(str.length());
                    pageDOS.writeBytes(str);
                    while (overflow-- > 0) {
                        pageDOS.write((byte) 0);
                    }
                }
                curPos = 0;
                if (c == '\n') {
                    fieldIndex = 0;
                } else {
                    fieldIndex++;
                }
            } else if (c == -1) {
                // EOF
                done = true;
            } else {
                // 为字段信息，而不是EOF或空白字符或分隔符
                buf[curPos++] = (char) c;
                continue;
            }
            // 写完整的页面记录需要把header写进页面
            if (recordCnt >= recordNum || done && recordCnt > 0 || done && pageNum > 0) {
                int i = 0;
                byte headerByte = 0;
                for (i=0; i<headerBitsNum; i++) {
                    if (i < recordCnt) {
                        headerByte |= (1 << (i % 8));
                    }
                    if ((i + 1) % 8 == 0) {
                        headerDOS.writeByte(headerByte);
                        headerByte = 0;
                    }
                }
                if (i % 8 > 0) {
                    headerDOS.writeByte(headerByte);
                }
                // 用0填补页面剩下的内容
                int rest = pageBytesNum - (recordCnt * recordBytesNum + headerBytesNum);
                for (i=0; i<rest; i++) {
                    pageDOS.writeByte(0);
                }
                // 写header和文件主体内容
                headerDOS.flush();
                headerBAOS.writeTo(os);
                pageDOS.flush();
                pageBAOS.writeTo(os);
                // 重新初始化文件流对象，为下一个页面做准备
                headerBAOS = new ByteArrayOutputStream(headerBytesNum);
                headerDOS = new DataOutputStream(headerBAOS);
                pageBAOS = new ByteArrayOutputStream(pageBytesNum);
                pageDOS = new DataOutputStream(pageBAOS);

                recordCnt = 0;
                pageNum++;
            }
        }
        br.close();
        os.close();
    }

}
