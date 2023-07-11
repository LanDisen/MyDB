package java.mydb.storage;

import java.util.*;

public interface DbFileIterator {

    void open();
    void close();
    boolean hasNext();
    Tuple next();

}
