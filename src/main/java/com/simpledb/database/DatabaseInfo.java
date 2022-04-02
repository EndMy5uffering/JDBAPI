package com.simpledb.database;

import java.io.File;

public class DatabaseInfo{
	
	private final String url;
	private final String user;
	private final String pass;
	private final String fileName;
	private final String directory;
	private final DatabaseType type;
	
	public DatabaseInfo(String url, String name, String pass){
		this.url = "jdbc:mysql://" + url;
		this.user = name;
		this.pass = pass;
		this.fileName = "";
		this.directory = "";
		this.type = DatabaseType.MYSQL;
	}
	
	
	public DatabaseInfo(String directory, String fileName){
		this.url = "jdbc:sqlite:" + directory + File.separator + fileName + ".sqlite";
		this.user = null;
		this.pass = null;
		this.fileName = fileName;
		this.directory = directory;
		this.type = DatabaseType.SQLITE;
	}
	
	public String getUrl() {
		return url;
	}

	public String getUser() {
		return user;
	}

	public String getPass() {
		return pass;
	}

	public String getFileName() {
		return fileName;
	}
	
	public String getDirectory() {
		return directory;
	}
	
	@Override
	public String toString() {
		return "URL:\t" + (url != null ? url : "-") + "\nName:\t" + (user != null ? user : "-");
	}


	public DatabaseType getType() {
		return type;
	}
}
