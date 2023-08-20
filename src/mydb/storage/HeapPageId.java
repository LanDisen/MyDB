package mydb.storage;

import java.util.Objects;

/**
 * HeapPage唯一的ID
 */
public class HeapPageId implements PageId {

    private final int tableId;
    private final int pageIndex;

    /**
     * HeapPageId构造函数，创建一个PageId以确定特定的页面和表
     * @param tableId 所引用的表的ID
     * @param pageIndex 该表的页面索引
     */
    public HeapPageId(int tableId, int pageIndex) {
        this.tableId = tableId;
        this.pageIndex= pageIndex;
    }

    @Override
    public int getTableId() {
        return tableId;
    }

    /**
     * @return 返回对应表关联的页面索引
     */
    @Override
    public int getPageIndex() {
        return pageIndex;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, pageIndex);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PageId)) {
            return false;
        }
        PageId pid = (PageId) o;
        if (tableId == pid.getTableId() && pageIndex == pid.getPageIndex()) {
            return true;
        }
        return false;
    }

    /**
     * @return 返回HeapPageId对象的序列化表示，用于写入磁盘（持久化）。
     *
     */
    @Override
    public int[] serialize() {
        int[] data = new int[2];
        data[0] = getTableId(); // 表ID
        data[1] = getPageIndex(); // 所在页面的索引
        return data;
    }
}
