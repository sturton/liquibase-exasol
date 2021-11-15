package liquibase.ext.exasol.statement;

public class CreateTableStatement extends liquibase.statement.core.CreateTableStatement {

    /**
     * Exasol has a CREATE OR REPLACE option for most items.
     **/
    private boolean replaceIfExists = false;


    public CreateTableStatement(String catalogName, String schemaName, String tableName) {
        super(catalogName,schemaName,tableName);
        this.replaceIfExists = false;
    }

    public CreateTableStatement(String catalogName, String schemaName, String tableName, boolean replaceIfExists) {
       super(catalogName,schemaName,tableName);
       this.replaceIfExists = replaceIfExists;
    }

    public boolean isReplaceIfExists() {
        return replaceIfExists;
    }

}
