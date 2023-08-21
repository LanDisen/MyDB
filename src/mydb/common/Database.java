package mydb.common;

import mydb.storage.BufferPool;
import mydb.storage.LogFile;

import java.io.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Database类用于初始化该数据库系统中的静态变量（catalog、buffer pool、log files等）
 */
public class Database {

    // AtomicReference保证并发操作时对该对象操作的原子性（线程安全）
    private static final AtomicReference<Database> instance =
            new AtomicReference<>(new Database());

    private final Catalog catalog;

    private final BufferPool bufferPool;

    private final static String LOG_FILE_NAME = "log";
    private final LogFile logFile;

    private Database() {
        catalog = new Catalog();
        bufferPool = new BufferPool(BufferPool.DEFAULT_PAGES_NUM);
        LogFile file = null;
        try {
            file = new LogFile(new File(LOG_FILE_NAME));
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        logFile = file;
    }

    /**
     * @return 获取数据库目录用来检索表
     */
    public static Catalog getCatalog() {
        return instance.get().catalog;
    }

    /**
     * @return 获得该数据库系统的缓冲池对象
     */
    public static BufferPool getBufferPool() {
        return instance.get().bufferPool;
    }

    /**
     * @return 返回该数据库系统的日志文件
     */
    public static LogFile getLogFile() {
        return instance.get().logFile;
    }
}
