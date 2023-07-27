package mydb.storage;

/**
 * Table¿‡
 */
public class Table {
    private DbFile dbFile;
    private String name;
    private String primaryKeyName;

    public Table(DbFile dbFile, String name, String primaryKeyName) {
        this.dbFile = dbFile;
        this.name = name;
        this.primaryKeyName = primaryKeyName;
    }

    public void setDbFile(DbFile dbFile) {
        this.dbFile = dbFile;
    }

    public DbFile getDbFile() {
        return dbFile;
    }

    void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    void setPrimaryKeyName(String primaryKeyName) {
        this.primaryKeyName = primaryKeyName;
    }

    public String getPrimaryKeyName() {
        return primaryKeyName;
    }
}