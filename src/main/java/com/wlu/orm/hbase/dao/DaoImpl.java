package com.wlu.orm.hbase.dao;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.exceptions.HBaseException;
import org.apache.hadoop.hbase.util.Bytes;

import com.wlu.orm.hbase.annotation.DatabaseField;
import com.wlu.orm.hbase.connection.HBaseConnection;
import com.wlu.orm.hbase.exceptions.HBaseOrmException;
import com.wlu.orm.hbase.schema.DataMapper;
import com.wlu.orm.hbase.schema.DataMapperFactory;
import com.wlu.orm.hbase.schema.IndexTable;
import com.wlu.orm.hbase.schema.value.StringValue;
import com.wlu.orm.hbase.schema.value.Value;
import com.wlu.orm.hbase.schema.value.ValueFactory;
import com.wlu.orm.hbase.util.Utils;

public class DaoImpl<T> implements Dao<T> {

	Log LOG = LogFactory.getLog(DaoImpl.class);
    Class<T> dataClass;
    private HBaseConnection hbaseConnection;
    // set constant schemas
    private DataMapperFactory<T> dataMapperFactory = null;

    public DaoImpl(Class<T> dataClass, HBaseConnection connection)
            throws HBaseOrmException {
        this.dataClass = dataClass;
        hbaseConnection = connection;
        dataMapperFactory = new DataMapperFactory<T>(dataClass);
    }

    @Override
    public void createTable() throws IOException {
        if (hbaseConnection.tableExists(dataMapperFactory.tablename)) {
            hbaseConnection.deleteTable(dataMapperFactory.tablename);
        }
        if (hbaseConnection.tableExists(dataMapperFactory.getIndexTable().getNameAsString())) {
            hbaseConnection.deleteTable(dataMapperFactory.getIndexTable().getNameAsString());
        }
        hbaseConnection.createTables(dataMapperFactory.tableCreateDescriptors());
        LOG.info(dataMapperFactory.tableCreateScript());
    }

    @Override
    public void createTableIfNotExist() throws IOException {
        if (hbaseConnection.tableExists(dataMapperFactory.tablename)) {
            LOG.info("The table has already existed, will not recreate it.");
            return;
        }
        hbaseConnection.createTables(dataMapperFactory.tableCreateDescriptors());
        LOG.info(dataMapperFactory.tableCreateScript());
    }

    @Override
    public void insert(T data) throws HBaseOrmException {
        // need to check the type
        if (!data.getClass().equals(dataClass)) {
            LOG.error("Class type of data is not the same as that of the Dao, should be "
                    + dataClass);
            return;
        }
        DataMapper<T> dataMapper = null;
        try {
            dataMapper = dataMapperFactory.create(data);
            dataMapper.insert(hbaseConnection);
        } catch (Exception e) {
            throw new HBaseOrmException(e);
        }
    }

    @Override
    public void insert(List<T> data) throws HBaseOrmException {
    	// need to check the type
    	if (!data.get(0).getClass().equals(dataClass)) {
    		throw new HBaseOrmException("Class type of data is not the same as that of the Dao, should be "
    				+ dataClass);
    	}
    	data.stream()
    		.forEach(a -> {
	    		try {
	    			DataMapper<T> dataMapper = null;
	    			dataMapper = dataMapperFactory.create(a);
	    			dataMapper.insert(hbaseConnection);
	    		} catch (Exception e) {
	    			throw new RuntimeException(e);
	    		}
    		});
    }

	private void validateType(List<T> data) throws HBaseOrmException {
		Type type = data.get(0).getClass();
    	if (type instanceof ParameterizedType) {
			ParameterizedType pType = (ParameterizedType)type;
			Class<?> clazz;
			try {
				clazz = Class.forName(pType.getActualTypeArguments()[0].getTypeName());
				if (!clazz.getClass().equals(dataClass)) {
					throw new HBaseOrmException("Class type of data is not the same as that of the Dao, should be "
		    				+ dataClass);
				}
			} catch (ClassNotFoundException e) {
				throw new HBaseOrmException(e);
			}
    	}else{
    		throw new HBaseOrmException("Class type of data is not the same as that of the Dao, should be "
    				+ dataClass);
    	}
	}
    
    @Override
    public void deleteById(Value rowkey) throws HBaseOrmException {
        org.apache.hadoop.hbase.client.Delete delete = new org.apache.hadoop.hbase.client.Delete(
                rowkey.toBytes());
        try {
            hbaseConnection.delete(Bytes.toBytes(dataMapperFactory.tablename),
                    delete);
        } catch (IOException e) {
            throw new HBaseOrmException(e);
        }
    }

    
    @Override
    public void deleteById(T data) throws HBaseOrmException {
        Value rowkey = null;
        try {
            rowkey = ValueFactory.create(Utils.getFromField(data,
                    dataMapperFactory.rowkeyField));
        } catch (Exception e) {
            throw new HBaseOrmException(e);
        }
        deleteById(rowkey);

    }

    @Override
    /**
     * The qualifier is pretty complicated
     */
    public void delete(T data, String FieldNameOfFamily,
                       String FieldNameOfqualifier) throws HBaseOrmException {
        if (FieldNameOfqualifier == null) {
            delete(data, FieldNameOfFamily);
            return;
        }
        Value rowkey;
        try {

            rowkey = ValueFactory.create(Utils.getFromField(data,
                    dataMapperFactory.rowkeyField));
            org.apache.hadoop.hbase.client.Delete delete = new org.apache.hadoop.hbase.client.Delete(
                    rowkey.toBytes());
            // get family name
            Field familyNameField = data.getClass().getDeclaredField(
                    FieldNameOfFamily);
            byte[] familyname = getFamilyByFieldName(familyNameField,
                    FieldNameOfFamily);
            // get qualifier name
            byte[] qualifiername = getQualiferByFamilyOrSublevelFieldName(
                    familyNameField, FieldNameOfqualifier);

            delete.deleteColumn(familyname, qualifiername);
            hbaseConnection.delete(Bytes.toBytes(dataMapperFactory.tablename),
                    delete);
        } catch (Exception e) {
            throw new HBaseOrmException(e);
        }
    }

    @Override
    public void delete(T data, String familyFieldName) throws HBaseOrmException {
        if (familyFieldName == null) {
            delete(data);
            return;
        }
        Value rowkey;
        try {
            rowkey = ValueFactory.create(Utils.getFromField(data,
                    dataMapperFactory.rowkeyField));
            org.apache.hadoop.hbase.client.Delete delete = new org.apache.hadoop.hbase.client.Delete(
                    rowkey.toBytes());
            // get family name
            Field familyNameField = data.getClass().getDeclaredField(
                    familyFieldName);
            byte[] familyname = getFamilyByFieldName(familyNameField,
                    familyFieldName);
            delete.deleteFamily(familyname);
            hbaseConnection.delete(Bytes.toBytes(dataMapperFactory.tablename),
                    delete);
        } catch (Exception e) {
            throw new HBaseOrmException(e);
        }
    }


    private byte[] getFamilyByFieldName(Field familyNameField,
                                        String familyFieldName) throws SecurityException,
            NoSuchFieldException {
        return dataMapperFactory.fixedSchema.get(familyNameField)
                .getFamily();
    }

    private byte[] getQualiferByFamilyOrSublevelFieldName(
            Field familyNameField, String FieldNameOfQualifier)
            throws HBaseOrmException {
        // if qualifier name is set with family name
        byte[] qualifiername = dataMapperFactory.fixedSchema.get(
                familyNameField).getQualifier();
        // qualifier is not directly set or set with a wrong value
        if (qualifiername == null
                || Bytes.compareTo(qualifiername,
                Bytes.toBytes(FieldNameOfQualifier)) != 0) {
            qualifiername = null;
        }
        if (qualifiername == null) {
            Map<String, byte[]> subFieldToQualifier = dataMapperFactory.fixedSchema
                    .get(familyNameField).getSubFieldToQualifier();
            if (subFieldToQualifier == null) {
                qualifiername = null;
            } else if (subFieldToQualifier.get(FieldNameOfQualifier) != null) {
                qualifiername = subFieldToQualifier.get(FieldNameOfQualifier);
            } else {
                throw new HBaseOrmException("The field '"
                        + FieldNameOfQualifier
                        + "' of sub level family class '"
                        + familyNameField.getName()
                        + "' is not set as qualifier");
            }
            // else qualifier is set with name of the field's name
            if (qualifiername == null) {
                qualifiername = Bytes.toBytes(FieldNameOfQualifier);
            }
        }
        return qualifiername;
    }

    @Override
    public void delete(T data) throws HBaseOrmException {
        deleteById(data);

    }

    @Override
    public void update(T data) throws HBaseOrmException {
        insert(data);

    }

    @Override
    public void update(T data, List<String> familyFieldName) throws HBaseOrmException {
        if (familyFieldName == null) {
            update(data);
            return;
        }
        try {
            DataMapper<T> dm = dataMapperFactory.createEmpty(data);
            dm.setRowKey(data);
            dm.setFieldValue(data, familyFieldName);
            dm.insert(hbaseConnection);
        } catch (Exception e) {
            throw new HBaseOrmException(e);
        }
    }

    @Override
    public T queryById(Value id) throws HBaseOrmException {
        DataMapper<T> dm = dataMapperFactory.createEmpty(dataClass);
        if (dm == null) {
            return null;
        }
        return dm.queryById(id, hbaseConnection);
    }

    @Override
    public List<T> queryByIndexTable(String key) throws HBaseOrmException {
    	LOG.info("########### queryByIndexTable");
        DataMapper<T> dm = dataMapperFactory.createEmpty(dataClass);
        if (dm == null) {
        	return null;
        }
        
        String[] rowKey = key.split(IndexTable.KEY_SPLIT);
        Field field = getField(rowKey[0]);
        
   		boolean annotation = field.isAnnotationPresent(DatabaseField.class);
   		if(!annotation){
   			throw new HBaseOrmException("Field " + field.getName() + "is not indexed");
   		}
        List<String> rowKeyList = dm.queryByIndexTable(field, new StringValue(rowKey[1]), hbaseConnection);
        List<T> collect = rowKeyList.stream().map(m -> {
        	try {
        		LOG.debug("Find by key:" + m);
				return queryById(new StringValue(m));
			} catch (Exception e) {
				LOG.error("Error trying to query by index table", e);
				return null;
			}
        }).collect(Collectors.toList());
        return collect;
    }

    @Override
    public List<T> findAll() throws HBaseOrmException {
        DataMapper<T> dm = dataMapperFactory.createEmpty(dataClass);
        if (dm == null) {
            return null;
        }
        return dm.findAll(hbaseConnection);
    }
    
	private Field getField(String rowKey) throws HBaseOrmException {
		Field field = null;
        try {
        	field = dataClass.getDeclaredField(rowKey);
		} catch (NoSuchFieldException e) {
			throw new HBaseOrmException(e);
		} catch (SecurityException e) {
			throw new HBaseOrmException(e);
		}
		return field;
	}
    
    @Override
    public List<T> queryWithFilter(String filter, boolean returnWholeObject) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<T> queryWithFilter(String filter) {
        // TODO Auto-generated method stub
        return null;
    }

}
