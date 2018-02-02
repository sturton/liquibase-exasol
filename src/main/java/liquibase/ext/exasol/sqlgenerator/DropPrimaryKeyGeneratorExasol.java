/**
 * 
 */
package liquibase.ext.exasol.sqlgenerator;

import liquibase.database.Database;
import liquibase.ext.exasol.database.ExasolDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.DropPrimaryKeyGenerator;
import liquibase.statement.core.AddColumnStatement;
import liquibase.statement.core.DropPrimaryKeyStatement;
import liquibase.structure.core.PrimaryKey;

/**
 * Exasol doesn't really support it
 *
 */
public class DropPrimaryKeyGeneratorExasol extends DropPrimaryKeyGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

	@Override
	public boolean supports(DropPrimaryKeyStatement statement, Database database) {
		return database instanceof ExasolDatabase;
	}

	@Override
	public Sql[] generateSql(DropPrimaryKeyStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
		String schemaName = statement.getSchemaName();

		return new Sql[] {new UnparsedSql("DROP INDEX " + database.escapeObjectName(statement.getConstraintName(), PrimaryKey.class) +" ON " + database.escapeTableName(statement.getCatalogName(), schemaName, statement.getTableName()) ) };
	}
}
