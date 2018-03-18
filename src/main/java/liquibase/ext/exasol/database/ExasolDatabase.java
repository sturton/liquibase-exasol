/**
 * Copyright 2010 Open Pricer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package liquibase.ext.exasol.database;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import liquibase.CatalogAndSchema;
import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.DatabaseConnection;
import liquibase.database.OfflineConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.executor.ExecutorService;
import liquibase.logging.LogFactory;
import liquibase.statement.DatabaseFunction;
import liquibase.statement.core.RawCallStatement;
import liquibase.statement.core.RawSqlStatement;

import liquibase.structure.core.Catalog;
import liquibase.structure.core.Schema;

/**
 * Exasol implementation for liquibase
 *
 */
public class ExasolDatabase extends AbstractJdbcDatabase {
    public static final String PRODUCT_NAME = "EXASolution";

	private String databaseName=null;

	//private String defaultSchemaName=null;

    public ExasolDatabase() {
        super.unquotedObjectsAreUppercased=true;
        super.setCurrentDateTimeFunction("SYSTIMESTAMP");
        // Setting list of Oracle's native functions
        dateFunctions.add(new DatabaseFunction("SYSDATE"));
        dateFunctions.add(new DatabaseFunction("SYSTIMESTAMP"));
        dateFunctions.add(new DatabaseFunction("CURRENT_TIMESTAMP"));
        //super.sequenceNextValueFunction = "%s.nextval";
        //super.sequenceCurrentValueFunction = "%s.currval";
    }


	protected String getDatabaseName(){
		if (null==databaseName && getConnection() != null && (!(getConnection() instanceof OfflineConnection))) {
			try {
				databaseName = (String) ExecutorService.getInstance().getExecutor(this).queryForObject(new RawSqlStatement("SELECT PARAM_VALUE FROM SYS.EXA_METADATA WHERE PARAM_NAME = 'databaseName'"), String.class);
			} catch (DatabaseException e) {
				e.printStackTrace();
			}
		}
		return databaseName;
	}

	@Override
	public boolean isCorrectDatabaseImplementation(DatabaseConnection conn) throws DatabaseException {
		return "EXASolution".equals(conn.getDatabaseProductName());
	}

	@Override
	public String getDefaultDriver(String url) {
		if (url.startsWith("jdbc:exa:"))
			return "com.exasol.jdbc.EXADriver";
		else
			return null;
	}

    @Override
    public String getShortName() {
        return "exasol";
    }

    @Override
    protected String getDefaultDatabaseProductName() {
        return "EXASolution";
    }

    @Override
    public Integer getDefaultPort() {
        return 8563;
    }

    @Override
    public String getJdbcCatalogName(CatalogAndSchema schema) {
        return null;
    }

    @Override
    public String getJdbcSchemaName(CatalogAndSchema schema) {
        //return correctObjectName(schema.getCatalogName() == null ? schema.getSchemaName() : schema.getCatalogName(), Schema.class);
        return correctObjectName(schema.getSchemaName() , Schema.class);
    }

    @Override
    public boolean jdbcCallsCatalogsSchemas() {
        //return true;
        return false;
    }
    


    @Override
	public boolean supportsInitiallyDeferrableColumns() {
		return true;
	}

	@Override
	public String getCurrentDateTimeFunction() {
		return "CURRENT_TIMESTAMP";
	}

	@Override
	public boolean supportsTablespaces() {
		return false;
	}

	@Override
	public int getPriority() {
		return PRIORITY_DATABASE;
	}

	@Override
	public boolean supportsDDLInTransaction() {
		return true;
	}

         @Override
        public boolean supportsSchemas() {
            return true;
            //return false;
        }
        /*
         @Override
        public boolean supportsCatalogs() {
            return false;
        }
        */
        
        /* * / 
	@Override
	public String getDefaultCatalogName() {
		return this.getDefaultSchemaName(); //getDatabaseName();
        }
        /* */

    
	@Override
	public String getLiquibaseCatalogName() {
		return null; 
        }
	
	/*
	@Override
	public String getDefaultCatalogName() {//NOPMD
            System.err.println("ExasolDatabase::getDefaultCatalogName=\""+super.getDefaultCatalogName()+"\"");
	    return getDefaultSchemaName() ; // return super.getDefaultCatalogName() == null ? null : super.getDefaultCatalogName().toUpperCase();
	}
	*/

	/**
	* Default Schema Name is the currently open schema.
	*/
	@Override
	public String getDefaultSchemaName() {
                System.err.println("ExasolDatabase::getDefaultSchemaName=\""+defaultSchemaName+"\"");
		if (null==defaultSchemaName && getConnection() != null && (!(getConnection() instanceof OfflineConnection))) {
			try {
				defaultSchemaName = (String) ExecutorService.getInstance().getExecutor(this).queryForObject(new RawSqlStatement("SELECT CURRENT_SCHEMA"), String.class);
			} catch (DatabaseException e) {
				e.printStackTrace();
			}
		}
                System.err.println("ExasolDatabase::getDefaultSchemaName is now =\""+defaultSchemaName+"\"");
		return defaultSchemaName;
	}

        /**
        * Set the Default Schema Name and open the schema.
        */
	@Override
	public void  setDefaultSchemaName(String schemaName) {
            System.err.println("ExasolDatabase::setDefaultSchemaName=\""+schemaName+"\"");
	    super.setDefaultSchemaName ( schemaName ) ;


	    if (null!=defaultSchemaName && getConnection() != null && (!(getConnection() instanceof OfflineConnection))) {
		    try {
			    ExecutorService.getInstance().getExecutor(this).execute(new RawSqlStatement("OPEN SCHEMA "+schemaName));
		    } catch (DatabaseException e) {
			    e.printStackTrace();
		    }
	    }
	}

	/**
	 * No sequence in Exasol
	 */
	@Override
	public boolean supportsSequences() {
		return false; 
	}

        
	/**
	 * Exasol supports Cascade Constraints
	 */
	@Override
	public boolean supportsDropTableCascadeConstraints() {
		return true;
	}

                
	/**
	 * No autoincrement in Exasol
	 */
	@Override
	public boolean supportsAutoIncrement() {
		return true;
	}

	/**
	 * Most frequent reserved keywords (full list in "Fundamentals" manual)
	 */
	@Override
	public boolean isReservedWord(String string) {
		boolean reserved =false;
		reserved = reserved || "VALUE".equalsIgnoreCase(string);
		reserved = reserved || "PASSWORD".equalsIgnoreCase(string);
		reserved = reserved || "TITLE".equalsIgnoreCase(string);
		reserved = reserved || "ENABLED".equalsIgnoreCase(string);
		reserved = reserved || "RANK".equalsIgnoreCase(string);
		reserved = reserved || "POSITION".equalsIgnoreCase(string);
		reserved = reserved || "YEAR".equalsIgnoreCase(string);
		reserved = reserved || "ACCOUNT".equalsIgnoreCase(string);
		return reserved;
	}

	/**
	 * Use JDBC escape syntax
	 */
	@Override
	public String getDateTimeLiteral(Timestamp date) {
		return "'"+new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS").format(date)+"'";
	}

	/**
	 * Use JDBC escape syntax
	 */
	@Override
	public String getDateLiteral(Date date) {
		return "'"+new SimpleDateFormat("yyyy-MM-dd").format(date)+"'";
	}

	/**
	 * Use JDBC escape syntax
	 */
	@Override
	public String getTimeLiteral(Time date) {
		return "'"+new SimpleDateFormat("hh:mm:ss.SSS").format(date)+"'";
	}




}
