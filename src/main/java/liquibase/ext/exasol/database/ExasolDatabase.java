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

import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import liquibase.CatalogAndSchema;
import liquibase.Scope;
import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.Database;
import liquibase.database.DatabaseConnection;
import liquibase.database.OfflineConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.executor.ExecutorService;
import liquibase.logging.LogFactory;
import liquibase.statement.DatabaseFunction;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawCallStatement;
import liquibase.statement.core.RawSqlStatement;

import liquibase.structure.core.Catalog;
import liquibase.structure.core.Schema;

/**
 * Exasol implementation for liquibase
 *
 */
public class ExasolDatabase extends AbstractJdbcDatabase implements Database {
    public static final String PRODUCT_NAME = "EXASolution";
	private static final String EXECUTOR_NAME = "jdbc";

	private String databaseName=null;


    public ExasolDatabase() {
        super.unquotedObjectsAreUppercased=true;
        super.setCurrentDateTimeFunction("SYSTIMESTAMP");
        // Setting list of Exasol's native functions
        dateFunctions.add(new DatabaseFunction("SYSDATE"));
        dateFunctions.add(new DatabaseFunction("SYSTIMESTAMP"));
        dateFunctions.add(new DatabaseFunction("CURRENT_TIMESTAMP"));
    }


	protected String getDatabaseName(){
		if (null==databaseName && getConnection() != null && (!(getConnection() instanceof OfflineConnection))) {
			try {
				databaseName = (String) Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor(EXECUTOR_NAME, this).queryForObject(new RawSqlStatement("SELECT PARAM_VALUE FROM SYS.EXA_METADATA WHERE PARAM_NAME = 'databaseName'"), String.class);
			} catch (DatabaseException e) {
				e.printStackTrace(System.err);
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
	    return getDefaultSchemaName() ; // return super.getDefaultCatalogName() == null ? null : super.getDefaultCatalogName().toUpperCase();
	}
	*/

	/**
	* Default Schema Name is the currently open schema.
	*/
	@Override
	public String getDefaultSchemaName() {
		if (null==defaultSchemaName && getConnection() != null && (!(getConnection() instanceof OfflineConnection))) {
			try {
				defaultSchemaName = (String) Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor(EXECUTOR_NAME, this).queryForObject(new RawSqlStatement("SELECT CURRENT_SCHEMA"), String.class);
			} catch (DatabaseException e) {
				e.printStackTrace(System.err);
			}
		}
		return defaultSchemaName;
	}

        /**
        * Set the Default Schema Name and open the schema.
        */
	@Override
	public void  setDefaultSchemaName(String schemaName) {
	    super.setDefaultSchemaName ( schemaName ) ;


	    if (null!=defaultSchemaName && getConnection() != null && (!(getConnection() instanceof OfflineConnection))) {
		    try {
				Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor(EXECUTOR_NAME, this).execute(new RawSqlStatement("OPEN SCHEMA "+schemaName));
		    } catch (DatabaseException e) {
			    e.printStackTrace(System.err);
		    }
	    }
	}

    /**
     * Used to obtain the connection schema name through a statement
     * Override this method to change the statement.
     * Only override this if getConnectionSchemaName is left unchanges or is using this method.
     * @see AbstractJdbcDatabase#getConnectionSchemaName()
     * @return
     */
    @Override
    protected SqlStatement getConnectionSchemaNameCallStatement(){
        return new RawCallStatement("SELECT current_schema");
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
		reserved = reserved || "ENABLED".equalsIgnoreCase(string);
		reserved = reserved || "RANK".equalsIgnoreCase(string);
		reserved = reserved || "POSITION".equalsIgnoreCase(string);
		reserved = reserved || "YEAR".equalsIgnoreCase(string);
		reserved = reserved || "ACCOUNT".equalsIgnoreCase(string);
		reserved = reserved || "STATE".equalsIgnoreCase(string);
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
		return getDateLiteral(new SimpleDateFormat("hh:mm:ss.SSS").format(date));
	}

    @Override
    protected boolean isTimeOnly(final String isoDate) {
	return 
		isoDate.length() == "hh:mm:ss".length()
		|| ( isoDate.lastIndexOf('.') > 0 
                     && isoDate.substring(0, isoDate.lastIndexOf('.')).length() == "hh:mm:ss".length() 
		   )
	;
    }

    /**
     * Return an Exasol date literal with the same value as a string formatted using ISO 8601.
     * <p/>
     * Convert an ISO8601 date string to one of the following results:
     * to_date('1995-05-23', 'YYYY-MM-DD')
     * to_date('1995-05-23 09:23:59', 'YYYY-MM-DD HH24:MI:SS')
     * <p/>
     * Implementation restriction:
     * Currently, only the following subsets of ISO8601 are supported:
     * YYYY-MM-DD
     * YYYY-MM-DDThh:mm:ss
     * hh:mm:ss
     */
    @Override
    public String getDateLiteral(String isoDate) {
        String normalLiteral = super.getDateLiteral(isoDate);

        if (isDateOnly(isoDate)) {
            StringBuffer val = new StringBuffer();
            val.append("to_date(");
            val.append(normalLiteral);
            val.append(", 'YYYY-MM-DD')");
            return val.toString();
        } else if (isTimeOnly(isoDate)) {
            StringBuffer val = new StringBuffer();
            val.append("to_timestamp(");
            val.append(normalLiteral);
            val.append(", 'HH24:MI:SS.FF9')");
            return val.toString();
        } else if (isTimestamp(isoDate)) {
            StringBuffer val = new StringBuffer(36);
            val.append("to_timestamp(");
            val.append(normalLiteral);
            val.append(", 'YYYY-MM-DD HH24:MI:SS.FF9')");
            return val.toString();
        } else if (isDateTime(isoDate)) {
            normalLiteral = normalLiteral.substring(0, normalLiteral.lastIndexOf('.')) + "'";

            StringBuffer val = new StringBuffer(36);
            val.append("to_timestamp(");
            val.append(normalLiteral);
            val.append(", 'YYYY-MM-DD HH24:MI:SS.FF9')");
            return val.toString();
        } else {
            return "UNSUPPORTED:" + isoDate;
        }
    }


    /**
     * Returns Exasol-specific auto-increment DDL clause.
     */
    @Override
    public String getAutoIncrementClause(final BigInteger startWith, final BigInteger incrementBy,
										 String generationType, Boolean defaultOnNull) {
        if (!supportsAutoIncrement()) {
            return "";
        }

        // generate an SQL:2003 standard compliant auto increment clause by default

        String autoIncrementClause = getAutoIncrementClause();

        return autoIncrementClause;
    }

    @Override
    protected String getAutoIncrementClause() {
        return "IDENTITY";
    }


}
