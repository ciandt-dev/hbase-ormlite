package com.wlu.orm.hbase.util;


import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;

import com.wlu.orm.hbase.annotation.DatabaseField;

public class Utils {
	
	private static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
	private static final ObjectMapper MAPPER = new ObjectMapper();

	private static String methodFromField(Field field, String prefix) {
		return prefix + field.getName().substring(0, 1).toUpperCase()
				+ field.getName().substring(1);
	}

	public static Method findGetMethod(Field field) {
		String methodName = methodFromField(field, "get");
		Method fieldGetMethod;
		try {
			fieldGetMethod = field.getDeclaringClass().getMethod(methodName);
		} catch (Exception e) {
			return null;
		}
		if (fieldGetMethod.getReturnType() != field.getType()) {
			return null;
		}
		return fieldGetMethod;
	}



	public static Method findSetMethod(Field field) {
		boolean throwExceptions = true;		
		String methodName = methodFromField(field, "set");
		Method fieldSetMethod;
		try {
			fieldSetMethod = field.getDeclaringClass().getMethod(methodName,
					field.getType());
		} catch (Exception e) {
			if (throwExceptions) {
				throw new IllegalArgumentException(
						"Could not find appropriate set method for " + field);
			} else {
				return null;
			}
		}
		if (fieldSetMethod.getReturnType() != void.class) {
			if (throwExceptions) {
				throw new IllegalArgumentException("Return type of set method "
						+ methodName + " returns "
						+ fieldSetMethod.getReturnType() + " instead of void");
			} else {
				return null;
			}
		}
		return fieldSetMethod;
	}

	public static <T> Object getFromField(T instance, Field field)
			throws IllegalArgumentException, IllegalAccessException,
			InvocationTargetException {
		Method m = findGetMethod(field);
		if(m == null){
			return null;
		}
		return m.invoke(instance);
	}

	public static <T> void setToField(T instance, Field field, Object value)
			throws IllegalArgumentException, IllegalAccessException,
			InvocationTargetException {
		Method m = findSetMethod(field);
		DatabaseField annotation = field.getAnnotation(DatabaseField.class);
		if(annotation.isSerialized()){
			if(field.getType().equals(List.class)){
				try {
					Class<?> clazz = null;
					Type genericType = field.getGenericType();
					if (genericType instanceof ParameterizedType) {
						ParameterizedType pType = (ParameterizedType)genericType;
						clazz = Class.forName(pType.getActualTypeArguments()[0].getTypeName());
					}else
						clazz = field.getType();
					
					value = MAPPER.readValue(value.toString(), MAPPER.getTypeFactory().constructCollectionType(List.class, clazz));
				} catch (Exception e) {
					throw new RuntimeException("Error Trying to deserialize field");
				}
			} else if(field.getType().equals(Date.class)){
					value = new Date(Long.parseLong(value.toString()));
			}
		}
		m.invoke(instance, value);
	}

	public static void main(String args[]) throws IllegalArgumentException,
			SecurityException, IllegalAccessException,
			InvocationTargetException, NoSuchFieldException,
			NoSuchMethodException, InstantiationException {
//		User u = new User("string", new Profile("s1", "s2", "s3"), null, 123);
//		System.out.println(u.getProfile());
//		Class profileclazz = Profile.class;
//		Constructor constr = profileclazz.getDeclaredConstructor();
//		constr.setAccessible(true);
//		Object p = constr.newInstance();
//		SetToField(p, Profile.class.getDeclaredField("name"), "this is name");
//
//		SetToField(u, User.class.getDeclaredField("profile"), p);
//		System.out.println(u.getProfile());
	}
}
