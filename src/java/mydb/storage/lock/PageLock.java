package java.mydb.storage.lock;

import java.mydb.storage.PageId;
import java.mydb.transaction.TransactionId;

/**
 * ҳ����
 */
public class PageLock {

    // ��������
    private int type;

    // ������
    public static final int SHARE = 0;

    // ������
    public static final int EXCLUSIVE = 0;

    // ����ID
    private TransactionId transactionId;

    /**
     * ҳ�������캯��
     * @param type �������ͣ�������������SHARE������������EXCLUSIVE��
     * @param tid ����ID
     */
    public PageLock(int type, TransactionId tid) {
        this.type = type;
        this.transactionId = tid;
    }

    public int getType() {
        return this.type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public TransactionId getTransactionId() {
        return transactionId;
    }

    @Override
    public String toString() {
        return "PageLock{" +
                "type=" + type +
                ", transactionId=" + transactionId +
                "}";
    }
}
