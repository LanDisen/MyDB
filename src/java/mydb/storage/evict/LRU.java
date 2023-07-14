package java.mydb.storage.evict;

import java.mydb.storage.PageId;

/**
 * Less Recent Used
 */
public class LRU implements EvictStrategy {

    @Override
    public PageId getEvictPageId() {
        return null;
    }

}
