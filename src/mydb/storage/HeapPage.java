package mydb.storage;

import mydb.common.Database;
import mydb.common.DbException;
import mydb.transaction.TransactionId;

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
    /**
     * 当前空槽数量
     */
    int emptySlotsNum;

    byte[] oldData;
    private final Byte oldDataLock = (byte) 0;

    /**
     * 用于标记该页面是否dirty
     */
    boolean dirty;

    /**
     * 正在对该页面进行操作的事务ID
     */
    TransactionId tid;

    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.tupleDesc = Database.getCatalog().getTupleDesc(id.getTableId());
        this.slotsNum = getTuplesNum();
        this.emptySlotsNum = slotsNum;
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
        this.dirty = false;
        this.tid = null;
        setBeforeImage(); // 拷贝一份旧数据
    }

    /**
     * @return 返回该页面的元组（tuple）数量
     */
    private int getTuplesNum() {
        int pageSize = BufferPool.getPageSize(); // 页面字节大小
        // tupleDesc.getSize()为每个元组所占字节数量
        int tuplesNum = (int) Math.floor(pageSize * 8 * 1.0 / (tupleDesc.getSize() * 8 + 1));
        return tuplesNum;
    }

    /**
     * 计算该页面的头部在HeapFile中的字节数，每个元组占用tupleSize个字节
     * @return 返回计算得到的页面头部字节数
     */
    private int getHeaderSize() {
        // header为bitmap，一个tuple占一位
        int headerSize = (int) Math.ceil(getTuplesNum() * 1.0 / 8);
        return headerSize;
    }

    @Override
    public HeapPageId getId() {
        return pid;
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

    /**
     * 从该页面中删除指定的元组。需要相应更新页面的header
     * @param tuple 需要删除的元组
     * @throws DbException 若该元组不在页面中，或元组对应的槽为空，会抛出数据库异常
     */
    public void deleteTuple(Tuple tuple) throws DbException {
        RecordId recordId = tuple.getRecordId();
        int slotIndex = recordId.getTupleNo();
        if (recordId.getPageId() != this.pid || !isSlotUsed(slotIndex)) {
            throw new DbException("tuple is not in this page");
        }
        // 成功删除元组，对应的槽设置为未使用
        setSlotUsed(slotIndex, false);
        tuples[slotIndex] = null;
        emptySlotsNum++;
    }

    /**
     * 将指定的元组插入该页面，并相应更新页面header
     * @param tuple 需要新增的元组
     * @throws DbException 若页面已满（无空槽），或者TupleDesc不匹配会抛出异常
     */
    public void insertTuple(Tuple tuple) throws DbException {
        TupleDesc tempTupleDesc = tuple.getTupleDesc();
        if (getEmptySlotsNum() == 0) {
            throw new DbException("this page is full");
        }
        if (!tempTupleDesc.equals(this.tupleDesc)) {
            throw new DbException("the tupleDesc is not matched");
        }
        // 找一个空的槽插入新元组
        for (int i=0; i<slotsNum; i++) {
            if (!isSlotUsed(i)) {
                setSlotUsed(i, true);
                tuple.setRecordId(new RecordId(this.pid, i));
                this.tuples[i] = tuple;
                this.emptySlotsNum--;
                return;
            }
        }
    }

    /**
     * @return 页面dirty会返回对应的事务ID，否则返回null
     */
    @Override
    public TransactionId isDirty() {
        return dirty ? tid: null;
    }

    /**
     * 将该页面设为dirty或not dirty，并记录对应的事务ID
     * @param dirty true为设置dirty，false为设置not dirty
     * @param tid 事务ID
     */
    @Override
    public void setDirty(boolean dirty, TransactionId tid) {
        this.dirty = dirty;
        this.tid = tid;
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
        return this.emptySlotsNum;
    }

    /**
     * @param index 槽的索引
     * @return 若对应索引的槽已被使用则返回true，否则返回false
     */
    public boolean isSlotUsed(int index) {
        // 找到槽在bitmap中对应的位置
        int byteIndex = index / 8;
        int bitIndex = index % 8;
        int isUsed = (header[byteIndex] >> bitIndex) & 1;
        return (isUsed == 1);
    }

    /**
     * 用于标记槽是否被使用，同时会更新header
     * @param index 槽的索引
     * @param value true为将该槽设置为被使用，false为设置为空槽
     */
    private void setSlotUsed(int index, boolean value) {
        byte b = header[Math.floorDiv(index, 8)]; // 得到bitmap所在的一行字节
        byte mask = (byte) (1 << (index % 8));
        if (value) {
            // 将该槽设置为被使用
            header[Math.floorDiv(index, 8)] = (byte) (b | mask);
        } else {
            // 设置为空槽
            header[Math.floorDiv(index, 8)] = (byte) (b & (~mask));
        }
    }

    /**
     * @return 返回该页面所有元组的迭代器（不能返回空槽中的元组）
     */
    public Iterator<Tuple> iterator() {
        List<Tuple> tupleList = new ArrayList<>();
        // 将非空的槽对应元组加入到迭代器中
        for (int i=0; i<slotsNum; i++) {
            if (isSlotUsed(i)) {
                tupleList.add(this.tuples[i]);
            }
        }
        return tupleList.iterator();
    }

    @Override
    public HeapPage getBeforeImage() {
        try {
            byte[] tempOldData = null;
            synchronized (oldDataLock) {
                tempOldData = oldData;
            }
            return new HeapPage(pid, tempOldData);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return null;
    }

    @Override
    public void setBeforeImage() {
        synchronized (oldDataLock) {
            oldData = getPageData().clone();
        }
    }
}
