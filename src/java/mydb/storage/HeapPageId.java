package java.mydb.storage;

import java.util.Objects;

/**
 * HeapPage唯一的ID
 */
public class HeapPageId implements PageId {

    private final int tableId;
    private final int pageNumber;

    /**
     * HeapPageId构造函数，创建一个PageId以确定特定的页面和表
     * @param tableId 所引用的表的ID
     * @param pageNumber 该表的页面数量
     */
    public HeapPageId(int tableId, int pageNumber) {
        this.tableId = tableId;
        this.pageNumber = pageNumber;
    }

    @Override
    public int getTableId() {
        return tableId;
    }

    /**
     * @return 返回对应表关联的页面数量
     */
    @Override
    public int getPageNumber() {
        return pageNumber;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, pageNumber);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PageId)) {
            return false;
        }
        PageId pid = (PageId) o;
        if (tableId == pid.getTableId() && pageNumber == pid.getPageNumber()) {
            return true;
        }
        return false;
    }
}
