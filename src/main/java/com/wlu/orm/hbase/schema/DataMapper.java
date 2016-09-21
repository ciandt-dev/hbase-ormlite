package com.wlu.orm.hbase.schema;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonNode;

import com.wlu.orm.hbase.annotation.DatabaseField;
import com.wlu.orm.hbase.connection.HBaseConnection;
import com.wlu.orm.hbase.exceptions.HBaseOrmException;
import com.wlu.orm.hbase.schema.value.StringValue;
import com.wlu.orm.hbase.schema.value.Value;
import com.wlu.orm.hbase.schema.value.ValueFactory;
import com.wlu.orm.hbase.util.Utils;

public class DataMapper<T> {
	private static Log LOG = LogFactory.getLog(DataMapper.class);
    // fixed schema for the generic type T, copy from the factory
    private String tablename;

    private IndexTable indexTable;

    public Map<Field, FamilyQualifierSchema> fixedSchema;
    public Map<Field, FieldDataType> fieldDataType;
    public Field rowkeyField;
    public Class<?> dataClass;

    // private data for individual instance
    private Map<Field, FamilytoQualifersAndValues> datafieldsToFamilyQualifierValue;
    // private data for rowkey
    private Value rowkey;
    
    /**
     * Construct with fixed members as parameters
     */
    public DataMapper(String tablename,
                      Map<Field, FamilyQualifierSchema> fixedSchema,
                      Map<Field, FieldDataType> fieldDataType, Field rowkeyField,
                      Class<?> dataClass) {
        this.tablename = tablename;
        this.indexTable = new IndexTable(tablename);
        this.fieldDataType = fieldDataType;
        this.fixedSchema = fixedSchema;
        this.rowkeyField = rowkeyField;
        this.dataClass = dataClass;
    }

    // insert the instance to HBase
    public void insert(HBaseConnection connection) throws IOException {
        Put put = new Put(rowkey.toBytes());
        
        for (Field field : datafieldsToFamilyQualifierValue.keySet()) {
        	FamilytoQualifersAndValues familytoQualifersAndValues = datafieldsToFamilyQualifierValue.get(field);
			familytoQualifersAndValues.addToPut(put);
        	DatabaseField databaseField = field.getAnnotation(DatabaseField.class);
        	if(databaseField.isIndexed()){
        		indexTable.add(field, rowkey, familytoQualifersAndValues);
        	}
        }
        connection.insert(tablename, put, indexTable);
    }

    public T queryById(Value id, HBaseConnection connection) throws HBaseOrmException {
        byte[] rowkey = id.toBytes();
        return queryById(rowkey, connection);
    }
    
    public T queryById(byte[] rowkey, HBaseConnection connection) throws HBaseOrmException {
        Get get = new Get(rowkey);
        Result result = null;
        try {
            result = connection.query(Bytes.toBytes(tablename), get);

            return createObjectFromResult(result);
        } catch (Exception e) {
            throw new HBaseOrmException(e);
        }
    }
    
    public List<String> queryByIndexTable(Field field, Value id, HBaseConnection connection) throws HBaseOrmException {
        ResultScanner result = null;
        try {//browserName:Safari
        	byte[] prefix = indexTable.generateIndexRowKey(field, id, new StringValue(""));
			Scan scan = new Scan(prefix);
        	PrefixFilter prefixFilter = new PrefixFilter(prefix);
        	scan.setFilter(prefixFilter);
        	
            result = connection.queryPrefix(Bytes.toBytes(indexTable.getTableName()), scan);
            List<String> list = new ArrayList<String>();
            result.forEach(c -> {
            	list.add(Bytes.toString(c.value()));	
            });
            return list;
        } catch (Exception e) {
            throw new HBaseOrmException(e);
        }
    }
    
    private T createObjectFromResult(Result result) throws SecurityException,
            NoSuchMethodException, IllegalArgumentException,
            InstantiationException, IllegalAccessException,
            InvocationTargetException {
        Class<?> clazz = dataClass;
        Constructor<?> constr = clazz.getDeclaredConstructor();
        Object instance = constr.newInstance();
        for (Field field : clazz.getDeclaredFields()) {
        	LOG.info(field.getName());
            if (field.equals(rowkeyField)) {
                byte[] value = result.getRow();
                Object fieldinstance = ValueFactory.createObject(
                        field.getType(), value);
                Utils.setToField(instance, field, fieldinstance);
                continue;
            }
            
            // datatype info
            FieldDataType fdt = fieldDataType.get(field);
            // schema info
            FamilyQualifierSchema fqs = fixedSchema.get(field);

           DatabaseField df = field.getAnnotation(DatabaseField.class);
           if (fdt.isSkip()) {
                continue;
           }else if(df.isSerialized()){
            	byte[] value = result.getValue(fqs.getFamily(),
            			fqs.getQualifier());
            	if(value == null || value.length == 0)
            		continue;
            	
            	Object fieldinstance = ValueFactory.createObject(JsonNode.class,
            			value);
            	Utils.setToField(instance, field, fieldinstance);
            }else if (fdt.isPrimitive()) {
                byte[] value = result.getValue(fqs.getFamily(),
                        fqs.getQualifier());
                Class<?> fieldClazz = fdt.dataclass;
                // convert from byte[] to Object according to field clazz
                Object fieldinstance = ValueFactory.createObject(fieldClazz,
                        value);
                Utils.setToField(instance, field, fieldinstance);
            } else if (fdt.isList()) {
                // get qualifier names and add the the list
                NavigableMap<byte[], byte[]> qvmap = result.getFamilyMap(fqs
                        .getFamily());
                List<String> lst = new ArrayList<String>();
                for (byte[] q : qvmap.keySet()) {
                    lst.add(Bytes.toString(q));
                }
                Utils.setToField(instance, field, lst);
            } else if (fdt.isMap()) {
                // get qualifier-value map and put the map
                NavigableMap<byte[], byte[]> qvmap = result.getFamilyMap(fqs
                        .getFamily());
                Map<String, String> map2 = new HashMap<String, String>();
                for (byte[] q : qvmap.keySet()) {
                    map2.put(Bytes.toString(q), Bytes.toString(qvmap.get(q)));
                }
                Utils.setToField(instance, field, map2);
            } else if (fdt.isSubLevelClass()) {
                // get the qualifer-object....
                Object sublevelObj = createSubLevelObject(
                        fqs.getSubFieldToQualifier(), fdt,
                        result.getFamilyMap(fqs.getFamily()));
                Utils.setToField(instance, field, sublevelObj);
            }
        }

        @SuppressWarnings("unchecked")
        T RetObject = (T) instance;

        return RetObject;
    }

    private Object createSubLevelObject(
            Map<String, byte[]> subfieldToQualifier, FieldDataType fdt,
            NavigableMap<byte[], byte[]> map) throws SecurityException,
            NoSuchMethodException, IllegalArgumentException,
            InstantiationException, IllegalAccessException,
            InvocationTargetException {
        Class<?> fieldClazz = fdt.dataclass;
        Constructor<?> constr = fieldClazz.getDeclaredConstructor();
        Object fieldinstance = constr.newInstance();

        for (Field subField : fieldClazz.getDeclaredFields()) {
            FieldDataType subdatatype = fdt.getSubLevelDataType(subField);
            String fieldstringname = subField.getName();
            if (subdatatype.isSkip()) {
                continue;
            } else if (subdatatype.isPrimitive()) {
                byte[] value = map
                        .get(subfieldToQualifier.get(fieldstringname));
                if(value!= null && value.length > 0){
                	Class<?> subfieldClazz = subdatatype.dataclass;
                	// convert from byte[] to Object according to field clazz
                	Object subfieldinstance = ValueFactory.createObject(
                			subfieldClazz, value);
                	Utils.setToField(fieldinstance, subField, subfieldinstance);
                }
            } else if (subdatatype.isList()) {
                NavigableMap<byte[], byte[]> qvmap = map;
                List<String> lst = new ArrayList<String>();
                for (byte[] q : qvmap.keySet()) {
                    lst.add(Bytes.toString(q));
                }
                Utils.setToField(fieldinstance, subField, lst);
            } else if (subdatatype.isMap()) {
                NavigableMap<byte[], byte[]> qvmap = map;
                Map<String, String> map2 = new HashMap<String, String>();
                for (byte[] q : qvmap.keySet()) {
                    map2.put(Bytes.toString(q), Bytes.toString(qvmap.get(q)));
                }
                Utils.setToField(fieldinstance, subField, map2);
            } else {
                Utils.setToField(fieldinstance, subField, null);
            }
        }
        return fieldinstance;
    }

    /**
     * Copy from the fixed schema. All members used in the method are fixed
     * according to the <code>dataClass</code>
     *
     * @throws HBaseOrmException
     */
    public void copyToDataFieldSchemaFromFixedSchema() throws HBaseOrmException {
        datafieldsToFamilyQualifierValue = new HashMap<Field, FamilytoQualifersAndValues>();
        for (Field field : fixedSchema.keySet()) {
            FamilyQualifierSchema fqv = fixedSchema.get(field);
            if (fqv.getFamily() == null) {
                throw new HBaseOrmException("Family should not be null!");
            }
            // if(fqv.getQualifier()== null){
            FamilytoQualifersAndValues f2qvs = new FamilytoQualifersAndValues();
            f2qvs.setFamily(fqv.getFamily());
            datafieldsToFamilyQualifierValue.put(field, f2qvs);
            // }

        }
    }

    public Map<Field, FamilytoQualifersAndValues> getDatafieldsToFamilyQualifierValue() {
        return datafieldsToFamilyQualifierValue;
    }

    public void setDatafieldsToFamilyQualifierValue(
            Map<Field, FamilytoQualifersAndValues> datafieldsToFamilyQualifierValue) {
        this.datafieldsToFamilyQualifierValue = datafieldsToFamilyQualifierValue;
    }

    /**
     * Create a concret DataMapper instance by filling rowkey, family:qualifier
     * etc
     *
     * @param instance
     * @throws IllegalArgumentException
     * @throws HBaseOrmException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    public void copyToDataFieldsFromInstance(T instance)
            throws IllegalArgumentException, HBaseOrmException,
            IllegalAccessException, InvocationTargetException {
        for (Field field : instance.getClass().getDeclaredFields()) {
            // if is rowkey
            if (rowkeyField.equals(field)) {
                rowkey = ValueFactory
                        .create(Utils.getFromField(instance, field));
                continue;
            }
            FamilyQualifierSchema fq = fixedSchema.get(field);
            FieldDataType fdt = fieldDataType.get(field);
            // field not included for HBase
            if (fq == null) {
                continue;
            }

            DatabaseField df = field.getAnnotation(DatabaseField.class);
            if(df.isSerialized()){
            	Value value = ValueFactory.createSerializeble(Utils.getFromField(instance,
                        field));
                datafieldsToFamilyQualifierValue.get(field).add(
                        fq.getQualifier(), value);
            
             	//Primitive, family and qualifier name are both specified
            } else if (fq.getQualifier() != null) {
                Value value = ValueFactory.create(Utils.getFromField(instance,
                        field));
                datafieldsToFamilyQualifierValue.get(field).add(
                        fq.getQualifier(), value);
            } else {
                // user defined class or a list as family data <br/>
                // 1. user defined class, need to add fixed qualifer informtion
                // to the fixedField
                if (fdt.isSubLevelClass()/* databasetable.canBeFamily() */) {
                    Map<byte[], Value> qualifierValues = getQualifierValuesFromInstanceAsFamily(
                            Utils.getFromField(instance, field), fq, fdt);
                    datafieldsToFamilyQualifierValue.get(field).add(
                            qualifierValues);
                } else if (fdt.isList()/* databasefield.isQualiferList() */) {
                    @SuppressWarnings("unchecked")
                    List<String> list = (List<String>) (Utils.getFromField(
                            instance, field));

                    if (list == null) {
                        continue;
                    }
                    for (String key : list) {
                        String qualifier = key;
                        Value value = ValueFactory.create(qualifier);

                        datafieldsToFamilyQualifierValue.get(field).add(
                                value.toBytes(), value);
                    }
                } else if (fdt.isMap()) {
                    // 2. Map
                    // TODO
                }

            }
        }
    }

    /**
     * Just set the rowkey for the instance
     *
     * @param instance
     */
    public void setRowKey(T instance) {
        for (Field field : instance.getClass().getDeclaredFields()) {
            // if is rowkey
            if (rowkeyField.equals(field)) {
                try {
                    rowkey = ValueFactory.create(Utils.getFromField(instance,
                            field));
                } catch (IllegalArgumentException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
            break;
        }
    }

    public void setFieldValue(T instance, List<String> fieldName)
            throws IllegalArgumentException, IllegalAccessException,
            InvocationTargetException, HBaseOrmException {
        for (Field field : instance.getClass().getDeclaredFields()) {
            if (!fieldName.contains(field.getName())) {
                continue;
            }
            // if is rowkey
            if (rowkeyField.equals(field)) {
                rowkey = ValueFactory
                        .create(Utils.getFromField(instance, field));
                continue;
            }
            FamilyQualifierSchema fq = fixedSchema.get(field);
            FieldDataType fdt = fieldDataType.get(field);
            // field not included for HBase
            if (fq == null) {
                continue;
            }

            // Primitive, family and qualifier name are both specified
            if (fq.getQualifier() != null) {
                Value value = ValueFactory.create(Utils.getFromField(instance,
                        field));
                datafieldsToFamilyQualifierValue.get(field).add(
                        fq.getQualifier(), value);
            } else {
                // user defined class or a list as family data <br/>
                // 1. user defined class, need to add fixed qualifer informtion
                // to the fixedField
                if (fdt.isSubLevelClass()/* databasetable.canBeFamily() */) {
                    Map<byte[], Value> qualifierValues = getQualifierValuesFromInstanceAsFamily(
                            Utils.getFromField(instance, field), fq, fdt);
                    datafieldsToFamilyQualifierValue.get(field).add(
                            qualifierValues);
                } else if (fdt.isList()/* databasefield.isQualiferList() */) {
                    @SuppressWarnings("unchecked")
                    List<String> list = (List<String>) (Utils.getFromField(
                            instance, field));

                    if (list == null) {
                        continue;
                    }
                    for (String key : list) {
                        String qualifier = key;
                        Value value = ValueFactory.create(null);

                        datafieldsToFamilyQualifierValue.get(field).add(
                                Bytes.toBytes(qualifier), value);
                    }
                } else if (fdt.isMap()) {
                    // 2. Map
                    // TODO
                }

            }
        }

    }

    public void setFieldValue(T instance, String fieldName, String subFieldName) {

    }

    /**
     *
     */
    // public void SetA

    /**
     * Build a map {qualifier: value} from the object as family
     *
     * @param instance the object as family
     * @return
     * @throws HBaseOrmException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    private Map<byte[], Value> getQualifierValuesFromInstanceAsFamily(
            Object instance, FamilyQualifierSchema fqs, FieldDataType fdt)
            throws HBaseOrmException, IllegalArgumentException,
            IllegalAccessException, InvocationTargetException {
        if (instance == null) {
            return null;
        }

        Map<byte[], Value> qualifierValues = new HashMap<byte[], Value>();
        {
            for (Field field : instance.getClass().getDeclaredFields()) {
                DatabaseField databaseField = field
                        .getAnnotation(DatabaseField.class);
                if (fdt.isSkip()) {
                    // not included in database
                    continue;
                }
                Class<?> fieldType = field.getType();
                // 1. primitive type (actually include those UDF class, to which
                // we treat them as toString())
                if (fdt.getSubLevelDataType(field).isPrimitive()/*
                                                                 * fieldType.
																 * isPrimitive()
																 */) {
                    if (!fieldType.isPrimitive()) {
                        LOG.warn("This is not good: instance is not primitive nor List nor Map , but "
                                + fieldType + ". We use toString() as value.");
                    }
                    String qualifier = getDatabaseColumnName(
                            databaseField.qualifierName(), field);
                    Value value = ValueFactory.create(Utils.getFromField(
                            instance, field));
                    qualifierValues.put(Bytes.toBytes(qualifier), value);

                }
                // Map, maybe HashMap or other map, all converted to Map
                else if (fdt.getSubLevelDataType(field).isMap()) {
                    // get each key as qualifier and value as value
                    @SuppressWarnings("unchecked")
                    Map<String, Object> map = (Map<String, Object>) Utils
                            .getFromField(instance, field);
                    for (String key : map.keySet()) {
                        String qualifier = key;
                        Value value = ValueFactory.create(map.get(key));
                        qualifierValues.put(Bytes.toBytes(qualifier), value);
                    }

                }
                // List, maybe ArrayList or others list, all converted to List
                else if (fdt.getSubLevelDataType(field).isList()) {
                    // not good ...
                    @SuppressWarnings("unchecked")
                    List<String> list = (List<String>) (Utils.getFromField(
                            instance, field));
                    for (String key : list) {
                        String qualifier = key;
                        Value value = ValueFactory.create(null);
                        qualifierValues.put(Bytes.toBytes(qualifier), value);
                    }
                } else {
                    //
                }

            }
        }
        return qualifierValues;
    }

    private String getDatabaseColumnName(String string, Field field) {
        if (string.length() == 0) {
            LOG.info("Field "
                    + dataClass.getName()
                    + "."
                    + field.getName()
                    + " need to take care of ... field name is used as column name");
            return field.getName();
        }
        return string;
    }

	public String getTablename() {
		return tablename;
	}

	public IndexTable getIndexTable() {
		return indexTable;
	}
}
