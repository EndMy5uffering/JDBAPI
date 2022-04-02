package com.simpledb.database;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.simpledb.annotations.DatabaseField;
import com.simpledb.annotations.DatabaseObject;
import com.simpledb.exceptions.QueryObjectException;

public class QueryObject {
	
	//INSERT INTO {TABLE} (vname, ...) VALUES (value, ...)
	//SELECT * FROM {TABLE} WHERE {ARGS}
	//UPDATE {TABLE} SET {VALUES} WHERE {ARGS}
	//DELETE FROM {TABLE} WHERE {ARGS}
	
	private static Map<String, QueryConstructor> QuerryConstruction = new HashMap<>();
	private static Set<Class<?>> templateTypes = Set.of(int.class, boolean.class, byte.class, short.class, long.class, String.class, Integer.class, Boolean.class, Short.class, Long.class, Byte.class);
	
	
	private String fullQuery = "";
	private String commandName = "";
	private String tableName = "";
	private List<Pair<String, String>> ValueList = new ArrayList<>();
	
	public QueryObject(String commandName, String tableName) {
		this.commandName = commandName;
		this.tableName = tableName;
	}
	
	public QueryObject() {}
	
	/**
	 * Returns the fully constructed query as a string that can be send to the database.<br>
	 * If the query command like INSERT, DELETE, SELECT, ... has no query constructor set an exception will be thrown.<br>
	 * 
	 *  @exception NullPointerException Is thrown when no query constructor has been set for the query command.
	 * 
	 * */
	public String getQuery() {
		if(fullQuery == null || fullQuery == "")
			fullQuery = constructQuerry();
		return fullQuery;
	}
	
	private String constructQuerry() {
		QueryConstructor constructor = QuerryConstruction.get(commandName);
		if(constructor == null)
			throw new NullPointerException("Could not find a query for the given command name: " + commandName);
		return constructor.construct(this);
	}
	
	/**
	 * Will construct a value list from the given Pair list.<br>
	 * The output will be of the form:<br>
	 * <pre>	(column_name1, column_name2, ...) VALUES (value1, value2, ...)<pre><br>
	 * 
	 * @param args A pair list like the one from QueryObject.getValueList() or QueryObject.getArgumentList()
	 * */
	public static String constructValueList(List<Pair<String,String>> args) {
		String out = "(%s) VALUES ('%s')";
		String nextColumn = ",%s";
		String nextValue = "','%s";
		int valueCount = 0;
		for(Pair<String, String> p : args) {
			if(++valueCount > args.size() -1) nextColumn = nextValue = "";
			out = String.format(out, p.getFirst() + nextColumn, p.getSecond() + nextValue);
		}
		
		if(valueCount == 0) out = String.format(out, "", "");
		
		return out;
	}
	
	/**
	 * Will construct a key word list from the given Pair list.<br>
	 * The output will be of the form:<br>
	 * <pre>	column_name1='value1',column_name2='value2',...</pre><br>
	 * 
	 * @param args A pair list like the one from QueryObject.getValueList() or QueryObject.getArgumentList()
	 * */
	public static String constructKWargList(List<Pair<String,String>> args) {
		return constructKWargList(args, ",");
	}
	
	/**
	 * Will construct a key word list from the given Pair list.<br>
	 * The output will be of the form:<br>
	 * <pre>	column_name1='value1'{concatinator}column_name2='value2'{concatinator}...</pre><br>
	 * 
	 * @param args A pair list like the one from QueryObject.getValueList() or QueryObject.getArgumentList()
	 * @param concatinator A string that comes before the next pair in the list.
	 * */
	public static String constructKWargList(List<Pair<String,String>> args, String concatinator) {
		String out = "%s='%s'";
		String next = concatinator + "%s='%s'";
		int valueCount = 0;
		for(Pair<String, String> p : args) {
			if(++valueCount > args.size() -1) next = "";
			out = String.format(out, p.getFirst(), p.getSecond()) + next;
		}
		
		if(valueCount == 0) out = String.format(out, "", "");
		
		return out;
	}
	
	/**
	 * Sets the command name like SELECT, DELETE, INSERT, ...
	 * */
	public void setCommandName(String commandName) {
		this.commandName = commandName;
	}

	/**
	 * Sets the name of the table the query will be executed on.
	 * */
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	/**
	 * Adds values to the value list.
	 * 
	 * @param column The name of the column the value belongs to.
	 * @param value The value for the given column name.
	 * */
	public void addValue(String column, String value) {
		this.ValueList.add(new Pair<String, String>(column, value));
	}
	
	public void addValues(Object o) throws QueryObjectException {
		addValues(o, 0);
	}
	
	public void addValues(Object[] o, int... groups) throws QueryObjectException {
		for(Object obj: o) {
			if(groups.length > 0) {
				addValues(obj, groups);
			}else {
				addValues(obj, 0);
			}
		}
	}
	
	public void addValues(Object o, int... groups) throws QueryObjectException {
		Pair<List<Field>, List<Method>> fieldsMethods = getFieldsAndMethods(o, groups);
		List<Field> fields = fieldsMethods.getFirst();
		List<Method> methods = fieldsMethods.getSecond();
		
		for(Field f : fields) {
			String columnName = f.getAnnotation(DatabaseField.class).columnName();
			if(columnName.equals("") || columnName == null) columnName = f.getName();
			this.ValueList.add(new Pair<String,String>(columnName, getString(getValue(f, o))));
		}
		
		for(Method m : methods) {
			String columnName = m.getAnnotation(DatabaseField.class).columnName();
			if(columnName.equals("") || columnName == null) columnName = m.getName();
			this.ValueList.add(new Pair<String, String>(columnName, getString(targetInvocationWrapper(o, m))));
		}

	}
	
	private Pair<List<Field>, List<Method>> getFieldsAndMethods(Object o, int... groups) throws QueryObjectException{
		Set<Integer> groupsOfField = new HashSet<>();
		for(int i : groups) groupsOfField.add(i);
		List<Field> fields = new ArrayList<>();
		List<Method> methods = new ArrayList<>();
		if(!o.getClass().isAnnotationPresent(DatabaseObject.class)) {
			throw new QueryObjectException("Missing DatabaseObject annotation for object: " + o.getClass().getName());
		}

		for(Field f : o.getClass().getDeclaredFields()) {
			f.setAccessible(true);
			if(f.isAnnotationPresent(DatabaseField.class)) {
				if(templateTypes.contains(f.getType())) {
					if(DatabaseField.util.inSameGroup(groupsOfField, f.getAnnotation(DatabaseField.class).groups())) {
						fields.add(f);
					}
				}else {
					throw new QueryObjectException("Can not read field of type: " + f.getType().getSimpleName() + " as argument type in: " + o.getClass().getSimpleName());
				}
			}
		}

		for(Method m : o.getClass().getMethods()) {
			m.setAccessible(true);
			if(m.isAnnotationPresent(DatabaseField.class)) {
				if(DatabaseField.util.inSameGroup(groupsOfField, m.getAnnotation(DatabaseField.class).groups())) {
					if(m.getParameterCount() <= 0) {
						if(templateTypes.contains(m.getReturnType())) {
							methods.add(m);
						}else {
							throw new QueryObjectException("QueryObject error for: " + o.getClass().getSimpleName() + ".\nFunctions with @DatabaseFieldType can not have return type: " + m.getReturnType().getSimpleName());
						}
					}else {
						throw new QueryObjectException("Can not read function with @DatabaseFieldType annotation because it has to many argumetns. Functions with the @DatabaseFieldType annotation can not have any arguments");
					}
				}
			}
		}

		return new Pair<>(fields, methods);
	}

	public Object targetInvocationWrapper(Object target, Method method) throws QueryObjectException{
		try {
			return method.invoke(target);
		} catch (IllegalAccessException e) {
			throw new QueryObjectException("Could not access function: " + method.getName() + " on object: " + target.getClass().getName());
		} catch(IllegalArgumentException e){
			throw new QueryObjectException("IllegalArgumentException");
		} catch(InvocationTargetException e){
			throw new QueryObjectException("Target function caused an exception: " + e.getTargetException().getClass().getSimpleName() + " | with message: " + e.getTargetException().getMessage());
		}
	}
	
	private String getString(Object o) {
		try {
			return (String) o;
		} catch (Exception e) {
			return String.valueOf(o);
		}
	}
	
	private Object getValue(Field f, Object o) throws QueryObjectException {
		try {
			return f.get(o);
		} catch (IllegalArgumentException | IllegalAccessException e) {
			throw new QueryObjectException("Could not get value form field: " + f.getName());
		}
	}
	
	/**
	 * Sets the query of the query object.<br>
	 * 
	 * <b>Note:</b><br>
	 * When a query is set the constructor will no longer be called an instead the getQuery() function returns the given string.
	 * */
	public void setFullQuery(String query) {
		this.fullQuery = query;
	}
	
	/**
	 * Returns the value list.<br>
	 * All values are stored in a pair object with the column name and the value.<br>
	 * */
	public List<Pair<String, String>> getValueList(){
		return this.ValueList;
	}
	
	public String getTableName() {
		return this.tableName;
	}
	
	/**
	 * This factory function will construct a QueryObject containing the given query.
	 * */
	public static QueryObject getQueryObject(String query) {
		QueryObject queryObject = new QueryObject();
		queryObject.setFullQuery(query);
		return queryObject;
	}
	
	/**
	 * Adds a query constructor for a given command name like SELECT, DELETE, ...<br>
	 * When a query is constructed the constructor function looks for a QueryConstructor in a list for a fitting constructor for the query.<br>
	 * If no constructor was found for the command name a NullPointerException will be thrown.
	 * 
	 * @param commandName The name of the command like SELECT, DELETE, ... the constructor will be registered under.
	 * @param constructor A constructor function that gives a construction rule for all querys of that name.
	 * */
	public static void addQueryConstructor(String commandName, QueryConstructor constructor) {
		QueryObject.QuerryConstruction.put(commandName, constructor);
	}
}

class Pair<K, V>{
	
	private K first;
	private V second;
	
	public Pair(K first, V second) {
		this.first = first;
		this.second = second;
	}
	
	public K getFirst() {
		return first;
	}
	
	public V getSecond() {
		return second;
	}
	
}
