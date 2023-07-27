package java.mydb.storage;

import java.mydb.common.Database;
import java.mydb.common.DbException;
import java.mydb.transaction.TransactionId;

import java.io.*;
import java.util.*;


/**
 * 每个HeapPage实例都存放了HeapFiles的一个页面的数据
 * @see HeapFile
 * @see BufferPool
 */
public class HeapPage implements Page {

    final HeapPageId pid;

    final TupleDesc tupleDesc;

    /**
     * HeapPage的header本质是一个位图（bitmap），用于标记哪些槽（slot）为空或已使用
     */
    final byte[] header;

    /**
     * 存放在该页面的元组
     */
    final Tuple[] tuples;

    /**
     * 槽的数量
     */
    final int slotsNum;

    byte[] oldData;
    private final Byte oldDataLock = (byte) 0;

    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.tupleDesc = Database.getCatalog().getTupleDesc(id.getTableId());
        this.slotsNum = getTuplesNum();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
        // 对该页面的头部槽进行分配并读取
        this.header = new byte[getHeaderSize()];
        for (int i=0; i<header.length; i++) {
            header[i] = dis.readByte();
        }
        this.tuples = new Tuple[slotsNum];
        try {
            // 为该页面的元组分配空间并读取
            for (int i=0; i<tuples.length; i++) {
                tuples[i] = readNextTuple(dis, i);
            }
        } catch (NoSuchElementException e) {
            e.printStackTrace();
        }
        dis.close();
        //
    }

    /**
     * @return 返回该页面的元组（tuple）数量
     */
    private int getTuplesNum() {
        // TODO
        return 0;
    }

    /**
     * 计算该页面的头部在HeapFile中的字节数，每个元组占用tupleSize个字节
     * @return 返回计算得到的页面头部字节数
     */
    private int getHeaderSize() {
        // TODO
        return 0;
    }

    @Override
    public HeapPageId getId() {
        // TODO
        return null;
    }

    /**
     * 获得一个字节数组用于表示该页面的内容数据，用于对该页面序列化到磁盘
     * 可以将getPageData生成的字节数组传递给HeapPage构造函数，让它生成一个相同的HeapPage对象
     * @return 返回一个字节数组
     */
    @Override
    public byte[] getPageData() {
        int pageSize = BufferPool.getPageSize(); // 缓冲池使用的页面大小
        ByteArrayOutputStream baos = new ByteArrayOutputStream(pageSize);
        DataOutputStream dos = new DataOutputStream(baos);
        // 创建该页面的头部header
        for (byte b: header) {
            try {
                dos.writeByte(b);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        // 创建tuples
        for (int i=0; i<tuples.length; i++) {
            // 空槽
            if (!isSlotUsed(i)) {
                for (int j=0; j<tupleDesc.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                continue;
            }
            // 非空槽
            for (int j=0; j<tupleDesc.getFieldsNum(); j++) {
                Field field = tuples[i].getField(j);
                try {
                    field.serialize(dos);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        // 填补
        // pageSize - slotNum * tupleDesc.getSize()
        int zeroLen = BufferPool.getPageSize() - (header.length + tupleDesc.getSize() * tuples.length);
        byte[] zeros = new byte[zeroLen];
        try {
            dos.write(zeros, 0, zeroLen);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            // 数据写入完毕，将缓冲数据立即写到目标输出流中，不在缓冲区继续等待
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return baos.toByteArray();
    }

    /**
     * 用于添加新的空页面到文件中
     * 可以将该方法的返回结果传递到HeapFile的构造函数中用于创建空页面
     * @return 返回空页面的全0字节数组
     */
    public static byte[] createEmptyPageData() {
        int pageSize = BufferPool.getPageSize();
        return new byte[pageSize]; // 空页面用全0填充
    }

    public void deleteTuple(Tuple tuple) throws DbException {
        // TODO
    }

    public void insertTuple(Tuple tuple) throws DbException {
        // TODO
    }

    @Override
    public TransactionId isDirty() {
        // TODO
        return null;
    }

    @Override
    public void setDirty(boolean dirty, TransactionId tid) {
        // TODO
    }

    /**
     * 从源文件中获取下一个元组
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // 如果未设置关联位，则向前读取下一个元组，并返回null
        if (!isSlotUsed(slotId)) {
            // 遍历tupleDesc的字节
            for (int i=0; i<tupleDesc.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }
        // 读取tuple中的字段（fields）
        Tuple tuple = new Tuple(tupleDesc);
        RecordId recordId = new RecordId(pid, slotId);
        tuple.setRecordId(recordId);
        try {
            for (int i=0; i<tupleDesc.getFieldsNum(); i++) {
                Field field = tupleDesc.getFieldType(i).parse(dis);
                tuple.setField(i, field);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("field parse error");
        }
        return tuple;
    }

    /**
     * @return 返回页面中空槽的数量
     */
    public int getEmptySlotsNum() {
        // TODO
        return 0;
    }

    /**
     * @return 若槽已被使用则返回true，否则返回false
     */
    public boolean isSlotUsed(int index) {
        // TODO
        return false;
    }

    /**
     * 用于标记槽被使用
     */
    private void setSlotUsed(int index, boolean value) {
        // TODO
    }

    /**
     * @return 返回该页面所有元组的迭代器（不能返回空槽中的元组）
     */
    public Iterator<Tuple> iterator() {
        // TODO
        return null;
    }
}
