package java.mydb.storage;

import java.util.Objects;

/**
 * HeapPageΨһ��ID
 */
public class HeapPageId implements PageId {

    private final int tableId;
    private final int pageNumber;

    /**
     * HeapPageId���캯��������һ��PageId��ȷ���ض���ҳ��ͱ�
     * @param tableId �����õı��ID
     * @param pageNumber �ñ��ҳ������
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
     * @return ���ض�Ӧ�������ҳ������
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
