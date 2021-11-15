package liquibase.ext.exasol.sqlgenerator;

import liquibase.CatalogAndSchema;
import liquibase.database.Database;
import liquibase.ext.exasol.database.ExasolDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.GetViewDefinitionGenerator;
import liquibase.statement.core.GetViewDefinitionStatement;

public class GetViewDefinitionGeneratorExasol extends GetViewDefinitionGenerator {
    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(GetViewDefinitionStatement statement, Database database) {
        return database instanceof ExasolDatabase;
    }

    @Override
    public Sql[] generateSql(GetViewDefinitionStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        CatalogAndSchema schema = new CatalogAndSchema(statement.getCatalogName(), statement.getSchemaName()).customize(database);

        return new Sql[]{
                new UnparsedSql("SELECT VIEW_TEXT FROM EXA_ALL_VIEWS WHERE upper(VIEW_NAME)='" + statement.getViewName().toUpperCase() + "' AND VIEW_SCHEMA='" + schema.getSchemaName() + "'")
        };
    }
}
