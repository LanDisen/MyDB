package java.mydb.common;

import java.io.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Database类用于初始化该数据库系统中的静态变量（catalog、buffer pool、log files等）
 */
public class Database {

    // AtomicReference保证并发操作时对该对象操作的原子性（线程安全）
    private static final AtomicReference<Database> instance =
            new AtomicReference<>(new Database());

    //private final Catalog catalog;
}
