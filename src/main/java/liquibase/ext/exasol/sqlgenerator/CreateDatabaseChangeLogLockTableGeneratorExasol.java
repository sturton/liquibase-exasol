package liquibase.ext.exasol.sqlgenerator;

import liquibase.database.Database;
import liquibase.ext.exasol.database.ExasolDatabase;
import liquibase.sqlgenerator.SqlGenerator;
import liquibase.sqlgenerator.core.CreateDatabaseChangeLogLockTableGenerator;
import liquibase.statement.core.CreateDatabaseChangeLogLockTableStatement;

public class CreateDatabaseChangeLogLockTableGeneratorExasol extends CreateDatabaseChangeLogLockTableGenerator {

    @Override
    public boolean supports(CreateDatabaseChangeLogLockTableStatement statement, Database database) {
        return database instanceof ExasolDatabase;
    }

    //Datetime is mapped to timestamp - manifest when run against liquibase-core 3.5.3
    @Override
    protected String getDateTimeTypeString(Database database) {
        return "timestamp";
    }

    @Override
    public int getPriority() {
        return SqlGenerator.PRIORITY_DATABASE;
    }
}

