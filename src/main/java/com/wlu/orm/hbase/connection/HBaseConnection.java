package com.wlu.orm.hbase.connection;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

import com.wlu.orm.hbase.schema.IndexTable;


public class HBaseConnection implements Closeable{

    protected Connection connection;
    protected Admin admin;

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

    private void insert(byte[] tablename, List<Put> list) throws IOException {
        Table htable = connection.getTable(TableName.valueOf(tablename));
        try {
            htable.put(list);
        } finally {
            htable.close();
        }
	}
    
	public void insert(String tablename, Put put, IndexTable indexTable) throws IOException {
		insert(tablename.getBytes(),put);
		if(indexTable != null){
			String tableNameIndex = indexTable.getTableNameIndex();
			insert(tableNameIndex.getBytes(), indexTable.getPutList());
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

	@Override
	public void close() throws IOException {
		if(connection != null){
			connection.close();
		}
		if(admin != null){
			admin.close();
		}
	}


}
