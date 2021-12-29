package simpledb.database;

@FunctionalInterface
public interface QueryConstructor {
	
	public String construct(QueryObject q);
	
}
