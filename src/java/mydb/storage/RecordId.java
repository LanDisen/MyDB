package java.mydb.storage;

import java.io.Serial;
import java.io.Serializable;

/**
 * RecordId用于映射一个表的特定页面（Page）中特定元组（Tuple）
 * 一个RecordId可能会映射到多个元组
 */
public class RecordId implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * RecordId构造函数
     * @param pid page id
     * @param tupleNo page中元组号（tuple number）
     */
    public RecordId(PageId pid, int tupleNo) {
        // TODO
    }

    /**
     * @return 该RecordId引用的元组数（tuple number）
     */
    public int getTupleNum() {
        // TODO
        return 0;
    }

    /**
     * @return 返回对应的pageId
     */
    public PageId getPageId() {
        // TODO
        return null;
    }

    /**
     * 判断两个RecordId是否表示相同的元组（tuple）
     * @return 如果obj表示相同元组则返回true，反之false
     */
    @Override
    public boolean equals(Object obj) {
        // TODO
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * 两个相同的RecordId应该具有相同的hashCode
     */
    @Override
    public int hashCode() {
        // TODO
        throw new UnsupportedOperationException("unimplemented");
    }
}
