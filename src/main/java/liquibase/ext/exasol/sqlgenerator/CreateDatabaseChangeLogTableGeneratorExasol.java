package liquibase.ext.exasol.sqlgenerator;

import liquibase.database.Database;
import liquibase.ext.exasol.database.ExasolDatabase;
import liquibase.sqlgenerator.SqlGenerator;
import liquibase.sqlgenerator.core.CreateDatabaseChangeLogTableGenerator;
import liquibase.statement.core.CreateDatabaseChangeLogTableStatement;

public class CreateDatabaseChangeLogTableGeneratorExasol extends CreateDatabaseChangeLogTableGenerator {

    @Override
    public boolean supports(CreateDatabaseChangeLogTableStatement statement, Database database) {
        return database instanceof ExasolDatabase;
    }

    @Override
    protected String getDateTimeTypeString(Database database) {
        return "timestamp";
    }

    @Override
    public int getPriority() {
        return SqlGenerator.PRIORITY_DATABASE;
    }
}


