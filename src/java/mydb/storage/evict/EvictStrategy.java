package java.mydb.storage.evict;

import java.mydb.storage.PageId;

/**
 * �����������Խӿ�
 */
public interface EvictStrategy {

    /**
     *
     * @return
     */
    PageId getEvictPageId();
}
