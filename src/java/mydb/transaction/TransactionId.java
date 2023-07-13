package java.mydb.transaction;

import java.io.Serial;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

public class TransactionId implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * 多线程原子类作为计数器（counter），保证事务ID的原子性
     */
    static final AtomicLong counter = new AtomicLong(0);

    /**
     * 事务ID
     */
    final long id;

    public TransactionId() {
        id = counter.getAndIncrement();
    }

    public long getId() {
        return id;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TransactionId t = (TransactionId) obj;
        return (this.id == t.id);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (id ^ (id >>> 32));
        return result;
    }
}
