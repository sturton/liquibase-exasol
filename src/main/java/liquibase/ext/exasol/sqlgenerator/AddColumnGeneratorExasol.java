/**
 * 
 */
package liquibase.ext.exasol.sqlgenerator;

import liquibase.change.ColumnConfig;
import liquibase.database.Database;
import liquibase.database.core.SybaseASADatabase;
import liquibase.database.core.SybaseDatabase;
import liquibase.datatype.DataTypeFactory;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.datatype.core.DateTimeType;
import liquibase.datatype.core.DateType;
import liquibase.datatype.core.TimeType;
import liquibase.ext.exasol.database.ExasolDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.statement.core.AddColumnStatement;
import liquibase.statement.core.AddPrimaryKeyStatement;
import liquibase.statement.core.AddUniqueConstraintStatement;
import liquibase.structure.core.Column;
import liquibase.structure.core.Schema;
import liquibase.structure.core.Table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Exasol requires that "No other table options can be specified when a constraint is specified in the statement"
 */
public class AddColumnGeneratorExasol extends liquibase.sqlgenerator.core.AddColumnGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(AddColumnStatement statement, Database database) {
        return database instanceof ExasolDatabase;
    }



/*
    @Override
    private Sql[] generateMultipleColumns(List<AddColumnStatement> columns, Database database) {
        List<Sql> result = new ArrayList<Sql>();
        if (database instanceof MySQLDatabase) {
            String alterTable = generateSingleColumBaseSQL(columns.get(0), database);
            for (int i = 0; i < columns.size(); i++) {
                alterTable += generateSingleColumnSQL(columns.get(i), database);
                if (i < columns.size() - 1) {
                    alterTable += ",";
                }
            }
            result.add(new UnparsedSql(alterTable, getAffectedColumns(columns)));
        } else {
            for (AddColumnStatement column : columns) {
                result.addAll(Arrays.asList(generateSingleColumn(column, database)));
            }
        }
        return result.toArray(new Sql[result.size()]);
    }


    @Override
    protected String generateSingleColumnSQL(AddColumnStatement statement, Database database) {
        String alterTable = " ADD " + database.escapeColumnName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName(), statement.getColumnName()) + " " + DataTypeFactory.getInstance().fromDescription(statement.getColumnType() + (statement.isAutoIncrement() ? "{autoIncrement:true}" : ""), database).toDatabaseDataType(database);

        if (statement.isAutoIncrement() && database.supportsAutoIncrement()) {
            AutoIncrementConstraint autoIncrementConstraint = statement.getAutoIncrementConstraint();
            alterTable += " " + database.getAutoIncrementClause(autoIncrementConstraint.getStartWith(), autoIncrementConstraint.getIncrementBy());
        }

        alterTable += getDefaultClause(statement, database);

        if (!statement.isNullable()) {
            alterTable += " NOT NULL";
        } else {
            if (database instanceof SybaseDatabase || database instanceof SybaseASADatabase || database instanceof MySQLDatabase) {
                alterTable += " NULL";
            }
        }

        if (statement.isPrimaryKey()) {
            alterTable += " PRIMARY KEY";
        }

        if( database instanceof MySQLDatabase && statement.getRemarks() != null ) {
            alterTable += " COMMENT '" + statement.getRemarks() + "' ";
        }

        if (statement.getAddAfterColumn() != null && !statement.getAddAfterColumn().isEmpty()) {
            alterTable += " AFTER `" + statement.getAddAfterColumn() + "` ";
        }

        return alterTable;
    }


    @Override
    private String getDefaultClause(AddColumnStatement statement, Database database) {
        String clause = "";
        Object defaultValue = statement.getDefaultValue();
        if (defaultValue != null) {
	    clause += " DEFAULT " + DataTypeFactory.getInstance().fromObject(defaultValue, database).objectToSql(defaultValue, database);
        }
        return clause;
    }

/*
	@Override
	public Sql[] generateSql(AddColumnStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
		Iterator<String> columnIterator = statement.getColumns().iterator();

		String alterTable = "ALTER TABLE " + database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName()) +
                " ADD " +
                database.escapeColumnName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName(), statement.getColumnName())
                + " " + DataTypeFactory.getInstance().fromDescription(statement.getColumnType() + (statement.isAutoIncrement() ? "{autoIncrement:true}" : ""), database).toDatabaseDataType(database);

		if (statement.isAutoIncrement() && database.supportsAutoIncrement()) {
			alterTable += " " + database.getAutoIncrementClause(null, null);
		}

		if (statement.getDefaultValue()!=null){
			alterTable += " DEFAULT ";
			LiquibaseDataType defaultValueType = DataTypeFactory.getInstance().fromDescription(statement.getColumnType() + (statement.isAutoIncrement() ? "{autoIncrement:true}" : ""), database);
			alterTable +=(defaultValueType instanceof DateTimeType ?" TIMESTAMP ":(defaultValueType instanceof DateType ?" DATE ":(defaultValueType instanceof TimeType ?" TIME ":"")));
			alterTable += DataTypeFactory.getInstance().fromObject(statement.getDefaultValue(), database).objectToSql(statement.getDefaultValue(), database);
		}

		if (!statement.isNullable()) {
			alterTable += " NOT NULL";
		} else {
			if (database instanceof SybaseDatabase || database instanceof SybaseASADatabase) {
				alterTable += " NULL";
			}
		}

		List<Sql> returnSql = new ArrayList<Sql>();
		returnSql.add(new UnparsedSql(alterTable, new Column().setRelation(new Table().setName(statement.getTableName()).setSchema(new Schema(statement.getCatalogName(), statement.getSchemaName())).setName(statement.getColumnName()))));

		if (statement.isPrimaryKey()) {
			returnSql.addAll(Arrays.asList(SqlGeneratorFactory.getInstance().generateSql(new AddPrimaryKeyStatement(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName(), statement.getColumnName(), null) , database)));
		}

		if (statement.isUnique()) {
			returnSql.addAll(Arrays.asList(SqlGeneratorFactory.getInstance().generateSql(new AddUniqueConstraintStatement(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName(), new ColumnConfig[] { new ColumnConfig().setName(statement.getColumnName())}, null) , database)));
		}
		//	if (statement.getDefaultValue()!=null)
		//		returnSql.addAll(Arrays.asList(SqlGeneratorFactory.getInstance().generateSql(new AddDefaultValueStatement(statement.getSchemaName(), statement.getTableName(), statement.getColumnName(), statement.getColumnType(), statement.getDefaultValue()) , database)));

		addForeignKeyStatements(statement, database, returnSql);

		return returnSql.toArray(new Sql[returnSql.size()]);
	}
*/

}
