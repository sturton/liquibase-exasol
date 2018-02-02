package liquibase.ext.exasol.datatype;

import liquibase.database.Database;
import liquibase.datatype.DatabaseDataType;
import liquibase.ext.exasol.database.ExasolDatabase;

public class BooleanType extends liquibase.datatype.core.BooleanType {

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
        return new DatabaseDataType("BOOLEAN");
    }

    @Override
    protected boolean isNumericBoolean(Database database) {
        return true;
    }
}
