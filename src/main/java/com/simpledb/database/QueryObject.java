package com.simpledb.database;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.simpledb.annotations.DatabaseConstructor;
import com.simpledb.annotations.DatabaseField;
import com.simpledb.annotations.DatabaseObject;
import com.simpledb.exceptions.QueryObjectException;

public class QueryObject {
	
	//INSERT INTO {TABLE} (vname, ...) VALUES (value, ...)
	//SELECT * FROM {TABLE} WHERE {ARGS}
	//UPDATE {TABLE} SET {VALUES} WHERE {ARGS}
	//DELETE FROM {TABLE} WHERE {ARGS}
	
	public interface ConvertFrom{
		public String convert(Object o);
	}

	public interface ConvertTo{
		public Object convert(String o);
	}

	private static Map<String, QueryConstructor> QuerryConstruction = new HashMap<>();

	//private static Set<Class<?>> templateTypes = Set.of(int.class, boolean.class, byte.class, short.class, long.class, String.class, Integer.class, Boolean.class, Short.class, Long.class, Byte.class);
	
	private static Map<Class<?>, Pair<ConvertFrom, ConvertTo>> typeConverter = new HashMap<>();
	
	
	private String fullQuery = "";
	private String commandName = "";
	private String tableName = "";
	private List<Pair<String, String>> ValueList = new ArrayList<>();
	private DatabaseManager.AsyncCallback callback = null;
	private DatabaseManager.AsyncSQLExceptionHandle exceptionHandle = null;
	private static DatabaseManager.AsyncSQLExceptionHandle defaultExceptonHandle = (e) -> e.getSqlException().printStackTrace();

	static{
		typeConverter.put(int.class, new Pair<>((o) -> ""+o, (s) -> (int)Integer.valueOf(s)));
		typeConverter.put(boolean.class, new Pair<>((o) -> Boolean.toString((boolean)o), (s) -> (boolean)Boolean.parseBoolean(s)));
		typeConverter.put(byte.class, new Pair<>((o) -> Byte.toString((byte)o), (s) -> (byte)Byte.parseByte(s)));
		typeConverter.put(short.class, new Pair<>((o) -> Short.toString((short)o), (s) -> (short)Short.parseShort(s)));
		typeConverter.put(long.class, new Pair<>((o) -> Long.toString((long)o), (s) -> (long)Long.parseLong(s)));
		typeConverter.put(String.class, new Pair<>((o) -> (String)o, (s) -> s));
		typeConverter.put(Integer.class, new Pair<>((o) -> ((Integer)o).toString(), (s) -> Integer.parseInt(s)));
		typeConverter.put(Boolean.class, new Pair<>((o) -> ((Boolean)o).toString(), (s) -> Boolean.parseBoolean(s)));
		typeConverter.put(Short.class, new Pair<>((o) -> ((Short)o).toString(), (s) -> Short.parseShort(s)));
		typeConverter.put(Long.class, new Pair<>((o) -> ((Long)o).toString(), (s) -> Long.parseLong(s)));
		typeConverter.put(Byte.class, new Pair<>((o) -> ((Byte)o).toString(), (s) -> Byte.parseByte(s)));
		typeConverter.put(UUID.class, new Pair<>((o) -> ((UUID)o).toString(), (s) -> UUID.fromString(s)));
	}

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


    public static void registerConverter(Class<?> clazz, ConvertFrom from, ConvertTo to){
        typeConverter.put(clazz, new Pair<>(from, to));
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
			this.ValueList.add(new Pair<String,String>(columnName, typeConverter.get(f.getType()).getFirst().convert(getValue(f, o))));
		}
		
		for(Method m : methods) {
			String columnName = m.getAnnotation(DatabaseField.class).columnName();
			if(columnName.equals("") || columnName == null) columnName = m.getName();
			this.ValueList.add(new Pair<String, String>(columnName, typeConverter.get(m.getReturnType()).getFirst().convert(targetInvocationWrapper(o, m))));
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
				if(typeConverter.containsKey(f.getType())) {
					if(DatabaseField.util.inSameGroup(groupsOfField, f.getAnnotation(DatabaseField.class).groups())) {
						fields.add(f);
					}
				}else {
					throw new QueryObjectException("Can not read field of type: " + f.getType().getSimpleName() + " as argument type in: " + o.getClass().getSimpleName());
				}
			}
		}

		for(Method m : o.getClass().getDeclaredMethods()) {
			m.setAccessible(true);
			if(m.isAnnotationPresent(DatabaseField.class)) {
				if(DatabaseField.util.inSameGroup(groupsOfField, m.getAnnotation(DatabaseField.class).groups())) {
					if(m.getParameterCount() <= 0) {
						if(typeConverter.containsKey(m.getReturnType())) {
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

	public static <T> List<T> getFromResultSet(Class<T> returnType, ResultSet rs) throws QueryObjectException{
		Constructor<?> constructor = getDBConstructorForClass(returnType);
		if(constructor == null) throw new QueryObjectException("No suitable constructor found for class: " + returnType.getName());
		String[] fieldNames = constructor.getAnnotation(DatabaseConstructor.class).columnName();
		Class<?>[] typs = constructor.getParameterTypes();
		Object[] args = new Object[typs.length];
		List<T> returnlist = new ArrayList<>();

		try {
			while(rs.next()){
				int c = 0;
				for(String fn : fieldNames){
					String res;
					try {
						res = rs.getString(fn);
					} catch (SQLException e) {
						throw new QueryObjectException("Could not get field: " + fn + " from result set!");
					}
					if(res != null){
						args[c] = typeConverter.get(typs[c]).getSecond().convert(res);
					}
					++c;
				}
				try {
					returnlist.add(safeCast(constructor.newInstance(args), returnType));
				} catch (InstantiationException e) {
					throw new QueryObjectException("Could not instantiate object!");
				} catch (IllegalAccessException e) {
					throw new QueryObjectException("Could not access object constructor!");
				} catch (IllegalArgumentException e) {
					throw new QueryObjectException("Illegal arguments passed to constructor!");
				} catch (InvocationTargetException e) {
					throw new QueryObjectException("Construct throw an error: " + e.getTargetException().getMessage());
				}
			}
		} catch (SQLException e) {
			throw new QueryObjectException("Exception in result set!");
		}

		return returnlist;
	}

	private static Constructor<?> getDBConstructorForClass(Class<?> clazz){
		for(Constructor<?> c : clazz.getConstructors()){
			if(c.isAnnotationPresent(DatabaseConstructor.class)){
				return c;
			}
		}
		return null;
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
	
	private Object getValue(Field f, Object o) throws QueryObjectException {
		try {
			return f.get(o);
		} catch (IllegalArgumentException | IllegalAccessException e) {
			throw new QueryObjectException("Could not get value form field: " + f.getName());
		}
	}

	public void setAsyncCallback(DatabaseManager.AsyncCallback callback){
		this.callback = callback;		
	}

	protected DatabaseManager.AsyncCallback getCallback(){
		return this.callback;
	}
	
	protected boolean hasCallback(){
		return this.callback != null;
	}

	public void setAsyncExceptionHandle(DatabaseManager.AsyncSQLExceptionHandle handle){
		this.exceptionHandle = handle;
	}

	protected DatabaseManager.AsyncSQLExceptionHandle getExceptionHandle(){
		return this.exceptionHandle != null ? this.exceptionHandle : QueryObject.defaultExceptonHandle;
	}

	public void setDefaultAsyncExceptonHandle(DatabaseManager.AsyncSQLExceptionHandle handle){
		if(handle == null) throw new IllegalArgumentException("Default handle can not be null!");
		QueryObject.defaultExceptonHandle = handle;
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

	private static <T> T safeCast(Object o, Class<T> clazz) throws QueryObjectException {
		if(clazz == null)
			throw new QueryObjectException("Can not cast object to nullpointer!");
	    return clazz.isInstance(o) ? clazz.cast(o) : null;
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
