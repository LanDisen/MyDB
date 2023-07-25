package java.mydb.storage;

import java.mydb.transaction.TransactionId;

/**
 * Page用于表示BufferPool中的页面
 * DbFiles会从磁盘中读写Pages
 * 唯一的构造函数：Page(PageId id, byte[] data)
 */
public interface Page {

    /**
     * 获取该页面对应的唯一的PageId
     * PageId可用于查询存储于磁盘的Page（或查询Page是否存在）
     * @return 返回该页面的ID
     */
    PageId getId();

    /**
     * @return 返回存放在该页面的数据（byte数组）
     */
    byte[] getPageData();

    /**
     * 返回最近被修改（dirty）的页面对应的事务ID，不存在脏页面则返回null
     * 可用于判断是否存在脏页面
     * @return 若不存在脏页则返回null，否则返回对应事务ID
     */
    TransactionId isDirty();

    /**
     * 将该页面设置为脏，并记录最近将其修改的事务ID
     */
    void setDirty(boolean dirty, TransactionId tid);

}
