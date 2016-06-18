package simpledb;

import java.util.*;

import simpledb.TupleDesc.TDItem;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;
    String tableAlias;
    int tableid;
    TransactionId tid;
    DbFileIterator dbfi;
    TupleDesc tupleDesc;
    
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     * 
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid = tid;
        setSeqScan(tableid, tableAlias);
        
    }
    
    /**
     * 
     * helper method that is used set the background of this SeqScan
     * @param tableid table id
     * @param tableAlias table alias
     */
    private void setSeqScan(int tableid, String tableAlias) {
        this.tableid = tableid;
         this.tableAlias = tableAlias;
         Catalog catalog = Database.getCatalog();
         int tableIndex = catalog.findTableIndex(tableid);
         if (tableIndex == -1) {
            throw new NoSuchElementException("this table id does not exist");
         }
         DbFile tableContent = catalog.tableContents.get(tableIndex);
         this.tupleDesc = tableContent.getTupleDesc();
         dbfi = tableContent.iterator(tid);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableid);
    }
    
    /**
     * @return Return the alias of the table this operator scans. 
     * */
    public String getAlias()
    {
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        setSeqScan(tableid, tableAlias);
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        dbfi.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     * 
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        ArrayList<Type> newTypeArray1 = new ArrayList<Type>();
        ArrayList<String> newFieldNameArray1 = new ArrayList<String>();
        
        for (TDItem tdi: tupleDesc.descArray) {
            newTypeArray1.add(tdi.fieldType);
            newFieldNameArray1.add(getAlias() + "." + tdi.fieldName);
        }
        
        Type[] newTypeArray2 = new Type[newTypeArray1.size()];
        String[] newFieldNameArray2 = new String[newTypeArray1.size()];
        for (int i = 0; i < newTypeArray1.size(); i++) {
            newTypeArray2[i] = newTypeArray1.get(i);
            newFieldNameArray2[i] = newFieldNameArray1.get(i);
        }
        
        return new TupleDesc(newTypeArray2, newFieldNameArray2);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        return dbfi.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        return dbfi.next();
    }

    public void close() {
        dbfi.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        dbfi.rewind();
    }
}
