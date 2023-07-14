package java.mydb.storage.evict;

import java.mydb.storage.PageId;

/**
 * »º³å³ØÇýÖð²ßÂÔ½Ó¿Ú
 */
public interface EvictStrategy {

    /**
     *
     * @return
     */
    PageId getEvictPageId();
}
