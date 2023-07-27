package mydb;

import java.io.File;

import mydb.common.Database;
import mydb.common.Type;
import mydb.execution.SeqScan;
import mydb.storage.HeapFile;
import mydb.storage.Tuple;
import mydb.storage.TupleDesc;
import mydb.transaction.TransactionId;

public class Test {
    public static void main(String[] argv) {
        Type types[] = new Type[] {
                Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE
        };
        String names[] = new String[] {
                "field0", "field1", "field2"
        };
        TupleDesc tupleDesc = new TupleDesc(types, names);
        HeapFile table1 = new HeapFile(new File("data/data.txt"), tupleDesc);
        Database.getCatalog().addTable(table1, "test");

        TransactionId tid = new TransactionId();
        SeqScan seqScan = new SeqScan(tid, table1.getId());
        try {
            seqScan.open();
            while (seqScan.hasNext()) {
                Tuple tuple = seqScan.next();
                System.out.println(tuple);
            }
        } catch (Exception e) {
            System.out.println("Exception:" + e);
        }
    }
}
