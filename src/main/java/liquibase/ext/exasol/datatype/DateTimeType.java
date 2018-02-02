package liquibase.ext.exasol.datatype;

import liquibase.database.Database;
import liquibase.datatype.DatabaseDataType;
import liquibase.ext.exasol.database.ExasolDatabase;

public class DateTimeType extends liquibase.datatype.core.DateTimeType {

    @Override
    public boolean supports(Database database) {
        return database instanceof ExasolDatabase;
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public DatabaseDataType toDatabaseDataType(Database database) {
        return new DatabaseDataType("TIMESTAMP");
    }
}
