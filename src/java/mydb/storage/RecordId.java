package java.mydb.storage;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * RecordId用于映射一个表的特定页面（Page）中特定元组（Tuple）
 * 一个RecordId可能会映射到多个元组
 */
public class RecordId implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * 该记录（record）所在页面的ID
     */
    private final PageId pid;

    /**
     * 元组号（tuple number）
     */
    private final int tupleNo;

    /**
     * RecordId构造函数
     * @param pid page id
     * @param tupleNo page中元组号（tuple number）
     */
    public RecordId(PageId pid, int tupleNo) {
        this.pid = pid;
        this.tupleNo = tupleNo;
    }

    /**
     * @return 该RecordId引用的元组号（tuple number）
     */
    public int getTupleNo() {
        return tupleNo;
    }

    /**
     * @return 返回对应的pageId
     */
    public PageId getPageId() {
        return pid;
    }

    /**
     * 判断两个RecordId是否表示相同的元组（tuple）
     * @return 如果obj表示相同元组则返回true，反之false
     */
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof RecordId)) {
            return false;
        }
        RecordId recordId = (RecordId) obj;
        if (pid.equals(recordId.getPageId()) && tupleNo == recordId.getTupleNo()) {
            return true;
        }
        return false;
    }

    /**
     * 两个相同的RecordId应该具有相同的hashCode
     */
    @Override
    public int hashCode() {
        return Objects.hash(pid, tupleNo);
    }
}
