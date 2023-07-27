package mydb.storage;

/**
 * PageId用于确定表（table）的page
 * PageId为接口（interface）
 */
public interface PageId {

    //int[] serialize();

    /**
     * @return 通过PageId返回唯一的表ID
     */
    int getTableId();

    /**
     * @return 返回页面对应表所引用的页面索引位置
     */
    int getPageIndex();

    /**
     * @return 该Page的哈希值
     * 如果需要将PageId用于哈希表的key则必须实现
     */
    int hashCode();

    /**
     * 用于比较PageId是否相等（需要判断obj为PageId类）
     */
    boolean equals(Object obj);

}
