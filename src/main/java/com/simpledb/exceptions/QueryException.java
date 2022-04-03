package com.simpledb.exceptions;

import java.sql.SQLException;

import com.simpledb.database.QueryObject;

public class QueryException extends Exception{
    
    private QueryObject query;
    private SQLException sqlException;

    public QueryException(QueryObject query, SQLException sqlException){
        super(sqlException.getMessage());
        this.query = query;
        this.sqlException = sqlException;
    }

    public QueryObject getQuery(){
        return this.query;
    }

    public SQLException getSqlException(){
        return this.sqlException;
    }

}
