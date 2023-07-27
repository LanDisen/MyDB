package mydb.execution;

import java.io.Serial;
import mydb.storage.Tuple;
import mydb.storage.TupleDesc;
import java.util.NoSuchElementException;


/**
 * 操作符（Operator）的抽象类
 */
public abstract class Operator implements OpIterator{

    @Serial
    private static final long serialVersionUID = 1L;

    private Tuple next = null;
    private boolean isOpen = false;

    @Override
    public void open() {
        this.isOpen = true;
    }

    @Override
    public void close() {
        this.next = null;
        this.isOpen = false;
    }

    @Override
    public boolean hasNext() {
        if (this.next == null) {
            this.next = fetchNext();
        }
        return (this.next != null);
    }

    @Override
    public Tuple next() {
        if (this.next == null) {
            this.next = fetchNext();
            if (this.next == null) {
                throw new NoSuchElementException();
            }
        }
        Tuple tuple = this.next;
        this.next = null;
        return tuple;
    }

    /**
     * @return 返回迭代器的下一个Tuple（如果迭代结束则返回null）
     */
    protected abstract Tuple fetchNext();

    /**
     * @return 返回迭代器对应tuple的TupleDesc
     */
    public abstract TupleDesc getTupleDesc();

}
