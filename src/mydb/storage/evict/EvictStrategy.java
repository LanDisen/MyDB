package mydb.storage.evict;

import mydb.storage.PageId;

/**
 * 缓冲池驱逐策略接口
 */
public interface EvictStrategy {

    /**
     * 修改指定页面的数据后需要进行的策略
     * @param pid 进行修改的页面ID
     */
    void modifyData(PageId pid);

    /**
     * 获取将要驱逐的页面Id
     */
    PageId getEvictPageId();
}
