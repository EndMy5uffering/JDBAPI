package com.simpledb.database;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.LinkedBlockingQueue;

import com.simpledb.exceptions.DatabaseManagerException;
import com.simpledb.exceptions.QueryException;

public class DatabaseManager {
	
	public interface AsyncCallback{
		public void callback(ResultSet rs);
	}

	public interface AsyncSQLExceptionHandle{
		public void handle(QueryException exception);
	}

	private LinkedBlockingQueue<QueryObject> SQLStatements = new LinkedBlockingQueue<>();
	private Thread asyncWorker;
	private boolean running = true;
	
	private Connection connection;
	
	public DatabaseManager() {
		initWorker();
	}
	
	private void initWorker() {
		asyncWorker = new Thread(() -> {
			while(running) {
				QueryObject q = null;
				try {
					q = SQLStatements.take();
				} catch (InterruptedException e1) {
					if(running) e1.printStackTrace();
					running = false;
				}
				if(!running) return;
				if(q != null){
					try {
						ResultSet rs = connection.prepareStatement(q.getQuery()).executeQuery();
						if(q.hasCallback()) q.getCallback().callback(rs);
					} catch (SQLException e) {
						q.getExceptionHandle().handle(new QueryException(q, e));							
					}
				}
			}
		});
		asyncWorker.start();
	}
	
	/**
	 * This function sets up the database access.
	 * @throws DatabaseManagerException 
	 *  
	 * */
	public boolean createDatabaseConnection(DatabaseInfo info) throws DatabaseManagerException{
		if(info.getType() == DatabaseType.SQLITE) {
			File sqlitefile = new File(info.getDirectory(), File.separator + info.getFileName() + ".sqlite");
			if (!sqlitefile.exists()) {
				try {
					sqlitefile.createNewFile();
				} catch (IOException e) {
					throw new DatabaseManagerException("Could not create new save file!");
				}
			}
		}
		
		try {
			this.connection = getNewConnection(info);
		} catch (SQLException e) {
			throw new DatabaseManagerException("Could not connect to database: " + e.getMessage());
		}
		if(!testConnection(this.connection)) {
			throw new DatabaseManagerException("Connection to database failed!");
		} 
		return true;
	}
	
	public void closeConnection() throws DatabaseManagerException {
		this.running = false;
		if(this.connection != null) {
			try {
				this.connection.close();
			} catch (SQLException e) {
				throw new DatabaseManagerException("Exception while closing the connection: " + e.getMessage());
			}
		}
	}

	public void asyncSqlStatement(QueryObject query) throws DatabaseManagerException {
		if(!this.running) throw new DatabaseManagerException("Worker not running!");
		SQLStatements.add(query);
	}
	
	public boolean executeQuery(QueryObject query) throws QueryException {
		try {
			return connection.prepareStatement(query.getQuery()).execute();
		} catch (SQLException e) {
			throw new QueryException(query, e);
		}
	}

	public int executeUpdate(QueryObject query) throws QueryException{
		try {
			return connection.prepareStatement(query.getQuery()).executeUpdate();
		} catch (SQLException e) {
			throw new QueryException(query, e);
		}
	}
	
	private boolean testConnection(Connection con) {
		if(con == null) return false;
		try {
			if(con.prepareStatement("SELECT 1;").execute()) return true;
		} catch (SQLException e) {
			return false;
		}
		return false;
	}
	
	public boolean hasTable(String TableName) {
		try {
			return executeQuery(QueryObject.getQueryObject("SELECT * FROM " + TableName));
		} catch (QueryException e) {
			return false;
		}
	}
	
	public synchronized ResultSet getData(QueryObject query) throws SQLException {
		return connection.prepareStatement(query.getQuery()).executeQuery();
	}
	
	public static DatabaseInfo getDatabaseInfo(String url, String name, String pass) {
		return new DatabaseInfo(url, name, pass);
	}
	
	public static DatabaseInfo getDatabaseInfo(String directory, String fileName) {
		return new DatabaseInfo(directory, fileName);
	}
	
	private Connection getNewConnection(DatabaseInfo dbInfo) throws SQLException {
		return (dbInfo.getUser() != null && dbInfo.getPass() != null ? DriverManager.getConnection(dbInfo.getUrl(), dbInfo.getUser(), dbInfo.getPass()) : DriverManager.getConnection(dbInfo.getUrl()));
	}
	
}


