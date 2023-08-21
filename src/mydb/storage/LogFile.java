package mydb.storage;

import mydb.common.Database;
import mydb.transaction.TransactionId;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * 日志文件，用于实现数据库恢复系统。遵循预写日志（WAL）和两段锁协议。
 * LogFile类中很多方法都是synchronized，防止同时写同一个日志文件。
 *
 * 日志文件的格式如下所示：
 * 日志文件的第一个整数表示上次写入的检查点，若没有上次检查点则为-1。
 * 日志记录是变长的，日志文件的额外数据由日志记录组成。
 * 每个日志记录的开头都是一个Int类型的日志记录类型和一个Long类型的事务ID。
 * 每个日志记录的结尾都是一个偏移量整数，用于记录在日志文件中该记录的起始位置。
 * 日志记录有五种类型：ABORT、COMMIT、UPDATE、BEGIN、CHECKPOINT。
 * 其中ABORT、COMMIT、BEGIN类型的记录没有额外数据，UPDATE和CHECKPOINT有额外数据。
 * UPDATE记录：包括before和after数据库镜像用于保存更新前和更新后的页面。
 * CHECKPOINT记录：由创建检查点是的事务ID及其在磁盘上的第一个日志记录组成，该记录包括正在执行的事务数量、事务ID和首条记录的偏移量
 */
public class LogFile {

    final File file;

    private RandomAccessFile logFile;

    /**
     * 是否调用recovery()方法并记录到日志文件中。
     * true则恢复，false则不恢复。
     */
    boolean whetherToRecover;

    static final int ABORT_RECORD = 1;
    static final int COMMIT_RECORD = 2;
    static final int UPDATE_RECORD = 3;
    static final int BEGIN_RECORD = 4;
    static final int CHECKPOINT_RECORD = 5;
    static final int NO_CHECKPOINT_ID = -1;

    final static int INT_SIZE = 4;
    final static int LONG_SIZE = 8;

    long currentOffset = -1;

    int recordsNum = 0; // 统计日志记录的数量

    /**
     * 正在执行的事务（未ABORT或COMMIT）。
     * Key：事务ID；Value：该事务的第一个日志记录的位置
     */
    final Map<Long, Long> tidToFirstLogRecordMap = new HashMap<>();

    /**
     * LogFile构造函数，初始化指定的日志文件进行备份。
     * @param file 日志文件
     */
    public LogFile(File file) throws IOException {
        this.file = file;
        this.logFile = new RandomAccessFile(file, "rw");
        // 默认先不进行数据库恢复
        this.whetherToRecover = false;
    }

    /**
     * 即将要添加一个日志记录，进行初始化预处理操作
     */
    void preprocess() throws IOException {
        recordsNum++; // 新增日志记录
        if (!whetherToRecover) {
            whetherToRecover = true;
            logFile.seek(0);
            logFile.setLength(0);
            logFile.writeLong(NO_CHECKPOINT_ID);
            logFile.seek(logFile.length());
            currentOffset = logFile.getFilePointer();
        }
    }

    public synchronized int getRecordsNum() {
        return this.recordsNum;
    }

    /**
     * 将ABORT日志记录和对应事务写到日志文件中，在磁盘中进行备份，并进行rollback操作
     * @param tid 发生了abort的事务ID
     */
    public void logAbort(TransactionId tid) throws IOException {
        // 由于需要ROLLBACK，处理前首先用缓冲池锁住该过程
        synchronized (Database.getBufferPool()) {
            synchronized (this) {
                preprocess();
                // abort进行回滚
                rollback(tid);
                // 记录ABORT的事务以及存放在日志文件中的指针位置
                logFile.writeInt(ABORT_RECORD);
                logFile.writeLong(tid.getId());
                logFile.writeLong(currentOffset);
                currentOffset = logFile.getFilePointer();
                // 事务已异常结束
                force();
                tidToFirstLogRecordMap.remove(tid.getId());
            }
        }
    }

    /**
     * 将COMMIT日志记录和对应事务写到日志文件中，在磁盘中进行备份
     * @param tid 需要COMMIT的事务ID
     */
    public synchronized void logCommit(TransactionId tid) throws IOException {
        preprocess();
        logFile.writeInt(COMMIT_RECORD);
        logFile.writeLong(tid.getId());
        logFile.writeLong(currentOffset);
        currentOffset = logFile.getFilePointer();
        // 事务已提交结束，日志记录强制推送到磁盘中
        force();
        tidToFirstLogRecordMap.remove(tid.getId());
    }

    /**
     * 记录事务的开始
     * @param tid 待执行的事务ID
     */
    public synchronized void logTransactionBegin(TransactionId tid)
            throws IOException {
        if (tidToFirstLogRecordMap.get(tid.getId()) != null) {
            // 该事务已经开始
            System.err.println("logTransactionBegin: already began this transaction\n");
            throw new IOException("double logTransactionBegin()");
        }
        preprocess();
        logFile.writeInt(BEGIN_RECORD);
        logFile.writeLong(tid.getId());
        logFile.writeLong(currentOffset);
        tidToFirstLogRecordMap.put(tid.getId(), currentOffset);
        currentOffset = logFile.getFilePointer();
    }

    /**
     * 记录UPDATE日志记录到日志文件中
     * @param tid 进行UPDATE的事务ID
     * @param before UPDATE前的数据库镜像
     * @param after UPDATE后的数据库镜像
     */
    public synchronized void logUpdate(TransactionId tid, Page before, Page after)
        throws IOException {
        preprocess();
        logFile.writeInt(UPDATE_RECORD);
        logFile.writeLong(tid.getId());
        writePageData(logFile, before);
        writePageData(logFile, after);
        logFile.writeLong(currentOffset);
        currentOffset = logFile.getFilePointer();
    }

    /**
     * 将CHECKPOINT日志记录写入日志文件
     */
    public synchronized void logCheckpoint() throws IOException {
        // 确保已经对缓冲池上锁
        synchronized (Database.getBufferPool()) {
            synchronized (this) {
                preprocess();
                long startCheckpointOffset; // 检查点开始位置
                long endCheckpointOffset; // 检查点结束位置
                // 正在执行的事务ID集合
                Set<Long> transactionIds = tidToFirstLogRecordMap.keySet();
                Iterator<Long> transactionIdIterator = transactionIds.iterator();
                force();
                Database.getBufferPool().flushAllPages(); // 刷新缓冲池中的脏页
                startCheckpointOffset = logFile.getFilePointer();
                logFile.writeInt(CHECKPOINT_RECORD); // 日志记录类型
                logFile.writeLong(-1); // 无事务ID，用于占位
                logFile.writeInt(transactionIds.size()); // 正在执行的事务数量
                // 将所有执行的事务ID以及对应的最近日志记录偏移位置保存到日志文件
                while (transactionIdIterator.hasNext()) {
                    Long tid = transactionIdIterator.next();
                    logFile.writeLong(tid);
                    logFile.writeLong(tidToFirstLogRecordMap.get(tid));
                }
                // 写入检查点后，需要确保检查点位于日志文件起始位置
                endCheckpointOffset = logFile.getFilePointer();
                logFile.seek(0);
                logFile.writeLong(startCheckpointOffset);
                logFile.seek(endCheckpointOffset);
                logFile.writeLong(currentOffset);
                currentOffset = logFile.getFilePointer();
            }
        }
        logTruncate(); // 截断日志文件
    }

    /**
     * 截断（truncate）日志中任何不需要的部分以减少其空间消耗
     */
    public synchronized void logTruncate() throws IOException {
        preprocess();
        logFile.seek(0);
        long checkPointPos = logFile.readLong();
        long minLogRecord = checkPointPos; // 最小日志记录偏移量
        if (checkPointPos != -1) {
            logFile.seek(checkPointPos);
            int checkPointType = logFile.readInt(); // 日志记录类型
            long checkPointTid = logFile.readLong(); // 事务ID（占位）
            if (checkPointType != CHECKPOINT_RECORD) {
                // 读取的日志记录类型应该是CHECKPOINT，否则抛出异常
                throw new RuntimeException("Checkpoint pointer does not point to the checkpoint log record");
            }
            int transactionsNum = logFile.readInt(); // 正在执行的事务数量
            for (int i=0; i<transactionsNum; i++) {
                long tid = logFile.readLong(); // 事务ID
                long logRecordPos = logFile.readLong(); // 该事务最近的日志记录偏移位置
                if (logRecordPos < minLogRecord) {
                    minLogRecord = logRecordPos;
                }
            }
            // 临时待重写的日志文件
            File newFile = new File("~log" + System.currentTimeMillis());
            RandomAccessFile newLogFile = new RandomAccessFile(newFile, "rw");
            newLogFile.seek(0);
            newLogFile.writeLong((checkPointPos - minLogRecord) + LONG_SIZE);
            logFile.seek(minLogRecord);
            // 重写日志记录（因为truncate日志文件后会导致各个日志记录的偏移有所变化）
            while (true) {
                try {
                    int logRecordType = logFile.readInt();
                    long tid = logFile.readLong();
                    long newStart = newLogFile.getFilePointer(); // 日志记录的新位置
                    newLogFile.write(logRecordType);
                    newLogFile.writeLong(tid);
                    switch (logRecordType) {
                        case UPDATE_RECORD -> {
                            Page before = readPageData(logFile);
                            Page after = readPageData(logFile);
                            writePageData(newLogFile, before);
                            writePageData(newLogFile, after);
                        }
                        case CHECKPOINT_RECORD -> {
                            // 该检查点的正在执行事务的数量（与上面的transactionNum区分）
                            int tidNum = logFile.readInt();
                            while (tidNum-- > 0) {
                                long t = logFile.readLong(); // 事务ID
                                long offset = logFile.readLong(); // 该事务对应的最近日志记录偏移
                                newLogFile.writeLong(t);
                                newLogFile.writeLong(offset);
                            }
                        }
                        case BEGIN_RECORD -> {
                            // 新增事务
                            tidToFirstLogRecordMap.put(tid, newStart);
                        }
                    }
                    newLogFile.writeLong(newStart);
                    logFile.readLong();
                } catch (EOFException e) {
                    // 日志文件结束位置
                    break;
                }
            }
            // copy重写的日志文件
            logFile.close();
            file.delete();
            newFile.renameTo(file);
            logFile = new RandomAccessFile(file, "rw");
            logFile.seek(logFile.length());
            newFile.delete();
            currentOffset = logFile.getFilePointer();
        }
    }

    void writePageData(RandomAccessFile raf, Page page) throws IOException {
        PageId pid = page.getId();
        // Page类的构造函数所需要的参数列表
        int[] pageArgs = pid.serialize();
        // 存储页面信息的数据格式：
        // page class name
        // pid class name
        // pid class bytes // pageArgs构造函数参数个数
        // pid class data
        // page class bytes // 页面大小pageSize
        // page class data
        String pageClassName = page.getClass().getName();
        String pidClassName = pid.getClass().getName();
        logFile.writeUTF(pageClassName);
        logFile.writeUTF(pidClassName);
        logFile.writeInt(pageArgs.length); // bytes
        for (int arg: pageArgs) {
            logFile.writeInt(arg);
        }
        byte[] pageData = page.getPageData();
        logFile.writeInt(pageData.length);
        logFile.write(pageData);
    }

    Page readPageData(RandomAccessFile raf) throws IOException {
        PageId pid = null;
        Page page = null;
        String pageClassName = raf.readUTF();
        String pidClassName = raf.readUTF();
        try {
            Class<?> pageClass = Class.forName(pageClassName);
            Class<?> pidClass = Class.forName(pidClassName);
            Constructor<?>[] pidConstructors = pidClass.getDeclaredConstructors();
            int pidArgsNum = raf.readInt();
            // PageId的构造函数参数
            Object[] pidArgs = new Object[pidArgsNum];
            for (int i=0; i<pidArgsNum; i++) {
                pidArgs[i] = raf.readInt();
            }
            pid = (PageId) pidConstructors[0].newInstance(pidArgs);
            Constructor<?>[] pageConstructors = pageClass.getDeclaredConstructors();
            int pageSize = raf.readInt();
            byte[] pageData = new byte[pageSize];
            raf.read(pageData);
            Object[] pageArgs = new Object[2];
            pageArgs[0] = pid;
            pageArgs[1] = pageData;
            // 将构造函数的参数传入newInstance方法以创建Page实例化对象
            page = (Page) pageConstructors[0].newInstance(pageArgs);
        } catch (ClassNotFoundException | InvocationTargetException |
                IllegalAccessException | InstantiationException e) {
            e.printStackTrace();
            throw new IOException();
        }
        return page;
    }

    /**
     * 对事务进行回滚（ROLLBACK）操作，将该事务更新的任何页面的状态设置为预更新状态。
     * 不允许对已经提交的事务进行ROLLBACK。
     * @param tid 需要进行回滚的事务ID
     */
    public void rollback(TransactionId tid)
            throws IOException, NoSuchElementException {
        synchronized (Database.getBufferPool()) {
            synchronized (this) {
                preprocess();
                // 获取该事务对应最近日志记录的偏移量
                Long offset = tidToFirstLogRecordMap.get(tid.getId());
                // 读取日志记录
                logFile.seek(offset);
                Set<PageId> pageIdSet = new HashSet<>();
                while (true) {
                    if (logFile.getFilePointer() == logFile.length()) {
                        // 已读取到文件末尾，结束循环
                        break;
                    }
                    int logRecordType = logFile.readInt();
                    long transactionId = logFile.readLong();
                    if (logRecordType == UPDATE_RECORD) {
                        // 该记录为UPDATE，需要回滚至更新前的状态
                        Page beforePage = readPageData(logFile);
                        Page afterPage = readPageData(logFile);
                        PageId beforePageId = beforePage.getId();
                        // 如果已进行回滚则无需重复回滚
                        if (transactionId == tid.getId() && !pageIdSet.contains(beforePageId)) {
                            pageIdSet.add(beforePageId);
                            // 丢弃BufferPool中该pageId（视为未操作该页面）
                            Database.getBufferPool().discardPage(beforePageId);
                            // 将beforePage写回数据库（回滚）
                            Database.getCatalog().getDbFile(beforePageId.getTableId()).writePage(beforePage);
                        }
                    } else if (logRecordType == CHECKPOINT_RECORD) {
                        // 回滚CHECKPOINT日志记录（放弃该检查点）
                        int count = logFile.readInt(); // 事务数量
                        while (count-- > 0) {
                            logFile.readLong(); // 事务ID
                            logFile.readLong(); // 对应的日志记录偏移
                        }
                        logFile.readLong();
                    }
                }
                // 重新移动logFile文件指针的位置
                logFile.seek(logFile.length());
            }
        }
    }

    /**
     * 关闭日志系统，并保存所需要的状态信息以便可以快速重新启动
     */
    public synchronized void shutdown() {
        try {
            // 保存关闭日志系统时的检查点
            logCheckpoint();
            logFile.close();
        } catch (IOException e) {
            System.out.println("Error shutting down the log system");
            e.printStackTrace();
        }
    }

    /**
     * 恢复数据库系统，以确保数据库系统的一致性（已提交的事务会更新到磁盘，未提交的事务不会更新）
     */
    public void recover() throws IOException {
        synchronized (Database.getBufferPool()) {
            synchronized (this) {
                // 需要进行数据库恢复
                whetherToRecover = true;
                logFile.seek(0); // 检查点在日志文件的起始位置
                // 已提交事务ID集合
                Set<Long> commitIdSet = new HashSet<>();
                // UPDATE前的数据库镜像集合
                Map<Long, List<Page>> beforePages = new HashMap<>();
                // UPDATE后的数据库镜像集合
                Map<Long, List<Page>> afterPages = new HashMap<>();
                long checkPoint = logFile.readLong(); // 检查点
                while (true) {
                    if (logFile.getFilePointer() == logFile.length()) {
                        // logFile指针在文件末尾
                        break;
                    }
                    int logRecordType = logFile.readInt(); // 日志记录类型
                    long tid = logFile.readLong(); // 事务ID
                    switch (logRecordType) {
                        case UPDATE_RECORD -> {
                            // UPDATE日志记录
                            Page before = readPageData(logFile);
                            Page after = readPageData(logFile);
                            List<Page> beforeList = beforePages.getOrDefault(tid, new ArrayList<>());
                            beforeList.add(before);
                            beforePages.put(tid, beforeList);
                            List<Page> afterList = afterPages.getOrDefault(tid, new ArrayList<>());
                            afterList.add(after);
                            afterPages.put(tid, afterList);
                        }
                        case COMMIT_RECORD -> {
                            // COMMIT日志记录
                            commitIdSet.add(tid);
                        }
                        case CHECKPOINT_RECORD -> {
                            // CHECKPOINT日志记录
                            int count = logFile.readInt();
                            while (count-- > 0) {
                                logFile.readLong();
                                logFile.readLong();
                            }
                        }
                    }
                    logFile.readLong();
                }
                // 处理未提交的事务
                for (Long tid: beforePages.keySet()) {
                    if (!commitIdSet.contains(tid)) {
                        List<Page> pages = beforePages.get(tid); // 未提交事务处理的页面集合
                        // 恢复到操作前的页面
                        for (Page page: pages) {
                            Database.getCatalog().getDbFile(page.getId().getTableId()).writePage(page);
                        }
                    }
                }
                // 处理已提交的事务
                for (Long tid: commitIdSet) {
                    if (afterPages.containsKey(tid)) {
                        List<Page> pages = afterPages.get(tid); // 已提交事务处理的页面集合
                        // 写入提交后的页面
                        for (Page page: pages) {
                            Database.getCatalog().getDbFile(page.getId().getTableId()).writePage(page);
                        }
                    }
                }
            }
        }
    }

    /**
     * 用于打印具有可读性的日志记录信息，包括文件偏移位置、日志记录类型等信息
     */
    public void print() throws IOException {
        // 保存日志文件当前指针位置
        long curOffset = logFile.getFilePointer();
        logFile.seek(0);
        System.out.println("0: checkpoint record at offset " + logFile.readLong());
        while (true) {
            try {
                int logRecordType = logFile.readInt(); // 日志记录类型
                long checkPointTid = logFile.readLong(); // 事务ID
                // 打印日志记录类型和对应的事务ID
                System.out.println(logFile.getFilePointer() - (INT_SIZE + LONG_SIZE) +
                        ": RECORD TYPE " + logRecordType);
                System.out.println(logFile.getFilePointer() - LONG_SIZE + ": TID "
                        + checkPointTid);
                switch (logRecordType) {
                    case BEGIN_RECORD -> {
                        System.out.println("(BEGIN)");
                        System.out.println(logFile.getFilePointer() + ": RECORD START OFFSET: " + logFile.readLong());
                    }
                    case ABORT_RECORD -> {
                        System.out.println("(ABORT)");
                        System.out.println(logFile.getFilePointer() + ": RECORD START OFFSET: " + logFile.readLong());
                    }
                    case COMMIT_RECORD -> {
                        System.out.println("(COMMIT)");
                        System.out.println(logFile.getFilePointer() + ": RECORD START OFFSET: " + logFile.readLong());
                    }
                    case UPDATE_RECORD -> {
                        System.out.println("(UPDATE)");
                        long beforeStart = logFile.getFilePointer();
                        Page before = readPageData(logFile);
                        long afterStart = logFile.getFilePointer();
                        Page after = readPageData(logFile);

                        System.out.println(beforeStart + ": before image table id: " + before.getId().getTableId());
                        System.out.println((beforeStart + INT_SIZE + ": before image page index " + before.getId().getPageIndex()));
                        System.out.println((beforeStart + INT_SIZE) + " TO" + (afterStart - INT_SIZE) + ": page data");

                        System.out.println(afterStart + ": after image table id: " + after.getId().getTableId());
                        System.out.println((afterStart + INT_SIZE + ": after image page index " + after.getId().getPageIndex()));
                        System.out.println((afterStart + INT_SIZE) + " TO" + (logFile.getFilePointer()) + ": page data");

                        System.out.println(logFile.getFilePointer() + ": RECORD START OFFSET: " + logFile.readLong());

                    }
                    case CHECKPOINT_RECORD -> {
                        System.out.println("(CHECKPOINT)");
                        int transactionsNum = logFile.readInt(); // 正在执行的事务数量
                        System.out.println((logFile.getFilePointer() - INT_SIZE) + ": NUMBER OF EXECUTING TRANSACTIONS: " + transactionsNum);
                        while (transactionsNum-- > 0) {
                            long tid = logFile.readLong();
                            long recordOffset = logFile.readLong();
                            System.out.println(logFile.getFilePointer() - (LONG_SIZE + LONG_SIZE) + ": TID: " + tid);
                            System.out.println((logFile.getFilePointer() - LONG_SIZE) + ": FIRST LOG RECORD: " + recordOffset);
                        }
                        System.out.println(logFile.getFilePointer() + ": RECORD START OFFSET: " + logFile.readLong());
                    }
                }
            } catch (EOFException e) {
                // 已到日志文件末尾
                break;
            }
        }
        logFile.seek(curOffset);
    }

    public synchronized void force() throws IOException {
        // 将还未写入的数据（还在内存）全部强制推送到磁盘进行刷新
        logFile.getChannel().force(true);
    }
}
