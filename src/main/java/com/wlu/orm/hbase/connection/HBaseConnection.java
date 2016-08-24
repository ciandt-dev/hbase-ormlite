package com.wlu.orm.hbase.connection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.List;

public class HBaseConnection {

    private Connection connection;
    private Admin admin;

    public HBaseConnection() throws IOException {
        Configuration cfg = HBaseConfiguration.create();
        connection = ConnectionFactory.createConnection(cfg);
        admin = connection.getAdmin();
    }

    public HBaseConnection(Connection connection) throws IOException {
    	this.connection = connection;
        admin = connection.getAdmin();
    }
    
    public Connection getConnection(){
    	return connection;
    }
    
    /**
     * insert put to the table with name <code>tablename</code>
     *
     * @param tablename
     * @param put
     * @throws IOException
     */
    public void insert(byte[] tablename, Put put) throws IOException {
        Table htable = connection.getTable(TableName.valueOf(tablename));
        try {
            htable.put(put);
        } finally {
            htable.close();
        }
    }

    /**
     * Delete the whole row of table with name <code>tablename</code>
     *
     * @param tablename
     * @throws IOException
     */
    public void delete(byte[] tablename, Delete delete) throws IOException {
        Table htable =  connection.getTable(TableName.valueOf(tablename));
        try {
            htable.delete(delete);
        } finally {
            htable.close();
        }
    }

    public Result query(byte[] tablename, Get get) throws IOException {
        Table table =  connection.getTable(TableName.valueOf(tablename));
        Result result = null;
        try {
            result = table.get(get);

        } finally {
            table.close();
        }
        return result;

    }

    public boolean tableExists(final String tableName) throws IOException {
        return admin.tableExists(TableName.valueOf(tableName));
    }

    public void deleteTable(final String tableName) throws IOException {
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
    }

    public void createTable(HTableDescriptor td) throws IOException {
        admin.createTable(td);
    }

    public void createTables(List<HTableDescriptor> tds) throws IOException {
        for (HTableDescriptor td : tds) {
            this.createTable(td);
        }
    }
}
