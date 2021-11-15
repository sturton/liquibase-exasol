package liquibase.ext.exasol.snapshot.jvm;

import liquibase.Scope;
import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.Database;
import liquibase.ext.exasol.database.ExasolDatabase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.ext.exasol.database.ExasolDatabase;
import liquibase.logging.LogFactory;
import liquibase.snapshot.CachedRow;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.SnapshotGenerator;
import liquibase.snapshot.jvm.ColumnSnapshotGenerator;
import liquibase.snapshot.JdbcDatabaseSnapshot;
import liquibase.snapshot.jvm.JdbcSnapshotGenerator;
import liquibase.statement.DatabaseFunction;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Column;
import liquibase.structure.core.DataType;
import liquibase.structure.core.Relation;
import liquibase.structure.core.Table;
import liquibase.structure.core.View;
import liquibase.util.StringUtil;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import liquibase.database.OfflineConnection;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.snapshot.InvalidExampleException;
import liquibase.statement.core.RawSqlStatement;
import liquibase.structure.core.Schema;
import liquibase.util.SqlUtil;

public class ColumnSnapshotGeneratorExasol extends JdbcSnapshotGenerator {

    private static final String LIQUIBASE_COMPLETE = "liquibase-complete";
    private static final String EXECUTOR_NAME = "jdbc";


    public ColumnSnapshotGeneratorExasol() {
	super(Column.class, new Class[]{Table.class, View.class});
    }


    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        return database instanceof ExasolDatabase ? SnapshotGenerator.PRIORITY_ADDITIONAL : SnapshotGenerator.PRIORITY_NONE;
    }

    @Override
    public Class<? extends SnapshotGenerator>[] replaces() {
        Class<? extends SnapshotGenerator>[] replacedSnapshotGenerators  = ( Class<? extends SnapshotGenerator>[]) new Class[1] ;
       	replacedSnapshotGenerators[0] = ColumnSnapshotGenerator.class ;
        return replacedSnapshotGenerators ; 
    }


    protected Column readColumn(CachedRow columnMetadataResultSet, Relation table, Database database) throws SQLException, DatabaseException {

        String rawTableName = (String) columnMetadataResultSet.get("TABLE_NAME");
        String rawColumnName = (String) columnMetadataResultSet.get("COLUMN_NAME");
        String rawSchemaName = StringUtil.trimToNull((String) columnMetadataResultSet.get("TABLE_SCHEM"));
        String rawCatalogName = StringUtil.trimToNull((String) columnMetadataResultSet.get("TABLE_CAT"));
        String remarks = StringUtil.trimToNull((String) columnMetadataResultSet.get("REMARKS"));
        if (remarks != null) {
            remarks = remarks.replace("''", "'"); //come back escaped sometimes
        }
        Integer position = columnMetadataResultSet.getInt("ORDINAL_POSITION");


        Column column = new Column();
        column.setName(StringUtil.trimToNull(rawColumnName));
        column.setRelation(table);
        column.setRemarks(remarks);
        column.setOrder(position);

        Boolean nullable = columnMetadataResultSet.getBoolean("NULLABLE");
        if (nullable == null) {
            column.setNullable(false);
        } else {
            column.setNullable(nullable);
        }

        if (database.supportsAutoIncrement()) {
            if (table instanceof Table) {
                if (database instanceof ExasolDatabase) {
                    String data_default = StringUtil.trimToEmpty((String) columnMetadataResultSet.get("DATA_DEFAULT")).toLowerCase();
                    if (data_default.contains("iseq$$") && data_default.endsWith("nextval")) {
                        column.setAutoIncrementInformation(new Column.AutoIncrementInformation());
                    }
                } else {
                    if (columnMetadataResultSet.containsColumn("IS_AUTOINCREMENT")) {
                        String isAutoincrement = (String) columnMetadataResultSet.get("IS_AUTOINCREMENT");
                        isAutoincrement = StringUtil.trimToNull(isAutoincrement);
                        if (isAutoincrement == null) {
                            column.setAutoIncrementInformation(null);
                        } else if (isAutoincrement.equals("YES")) {
                            column.setAutoIncrementInformation(new Column.AutoIncrementInformation());
                        } else if (isAutoincrement.equals("NO")) {
                            column.setAutoIncrementInformation(null);
                        } else if (isAutoincrement.equals("")) {
                            Scope.getCurrentScope().getLog(getClass()).info("Unknown auto increment state for column " + column.toString() + ". Assuming not auto increment");
                            column.setAutoIncrementInformation(null);
                        } else {
                            throw new UnexpectedLiquibaseException("Unknown is_autoincrement value: '" + isAutoincrement + "'");
                        }
                    } else {
                        //probably older version of java, need to select from the column to find out if it is auto-increment
                        String selectStatement;
                        {
                            selectStatement = "select " + database.escapeColumnName(rawCatalogName, rawSchemaName, rawTableName, rawColumnName) + " from " + database.escapeTableName(rawCatalogName, rawSchemaName, rawTableName) + " where 0=1";
                        }
                        Scope.getCurrentScope().getLog(getClass()).fine("Checking " + rawTableName + "." + rawCatalogName + " for auto-increment with SQL: '" + selectStatement + "'");
                        Connection underlyingConnection = ((JdbcConnection) database.getConnection()).getUnderlyingConnection();
                        Statement statement = null;
                        ResultSet columnSelectRS = null;

                        try {
                            statement = underlyingConnection.createStatement();
                            columnSelectRS = statement.executeQuery(selectStatement);
                            if (columnSelectRS.getMetaData().isAutoIncrement(1)) {
                                column.setAutoIncrementInformation(new Column.AutoIncrementInformation());
                            } else {
                                column.setAutoIncrementInformation(null);
                            }
                        } finally {
                            try {
                                if (statement != null) {
                                    statement.close();
                                }
                            } catch (SQLException ignore) {
                            }
                            if (columnSelectRS != null) {
                                columnSelectRS.close();
                            }
                        }
                    }
                }
            }
        }

        DataType type = readDataType(columnMetadataResultSet, column, database);
        column.setType(type);

        Object defaultValue = readDefaultValue(columnMetadataResultSet, column, database);
        if (defaultValue != null && defaultValue instanceof DatabaseFunction && ((DatabaseFunction) defaultValue).getValue().matches("\\w+")) {
            defaultValue = new DatabaseFunction(((DatabaseFunction) defaultValue).getValue().toUpperCase());
        }
        column.setDefaultValue(defaultValue);


        return column;
    }



    @Override
    protected DatabaseObject snapshotObject(DatabaseObject example, DatabaseSnapshot snapshot) throws DatabaseException, InvalidExampleException {
        Database database = snapshot.getDatabase();
        Relation relation = ((Column) example).getRelation();
        if (((Column) example).getComputed() != null && ((Column) example).getComputed()) {
            return example;
        }
        Schema schema = relation.getSchema();

        List<CachedRow> columnMetadataRs = null;
        try {

            Column column = null;

            if (example.getAttribute(LIQUIBASE_COMPLETE, false)) {
                column = (Column) example;
                example.setAttribute(LIQUIBASE_COMPLETE, null);
            } else {
                JdbcDatabaseSnapshot.CachingDatabaseMetaData databaseMetaData = ((JdbcDatabaseSnapshot) snapshot).getMetaDataFromCache();

                columnMetadataRs = databaseMetaData.getColumns(((AbstractJdbcDatabase) database).getJdbcCatalogName(schema), ((AbstractJdbcDatabase) database).getJdbcSchemaName(schema), relation.getName(), example.getName());

                if (columnMetadataRs.size() > 0) {
                    CachedRow data = columnMetadataRs.get(0);
                    column = readColumn(data, relation, database);
                    setAutoIncrementDetails(column, database, snapshot);
                }
            }

            return column;
        } catch (Exception e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    protected void addTo(DatabaseObject foundObject, DatabaseSnapshot snapshot) throws DatabaseException, InvalidExampleException {
        if (!snapshot.getSnapshotControl().shouldInclude(Column.class)) {
            return;
        }
        if (foundObject instanceof Relation) {
            Database database = snapshot.getDatabase();
            Relation relation = (Relation) foundObject;
            List<CachedRow> allColumnsMetadataRs = null;
            try {

                JdbcDatabaseSnapshot.CachingDatabaseMetaData databaseMetaData = ((JdbcDatabaseSnapshot) snapshot).getMetaDataFromCache();

                Schema schema;

                schema = relation.getSchema();
                allColumnsMetadataRs = databaseMetaData.getColumns(((AbstractJdbcDatabase) database).getJdbcCatalogName(schema), ((AbstractJdbcDatabase) database).getJdbcSchemaName(schema), relation.getName(), null);

                for (CachedRow row : allColumnsMetadataRs) {
                    Column column = readColumn(row, relation, database);
                    setAutoIncrementDetails(column, database, snapshot);
                    column.setAttribute(LIQUIBASE_COMPLETE, true);
                    relation.getColumns().add(column);
                }
            } catch (Exception e) {
                throw new DatabaseException(e);
            }
        }

    }

    protected void setAutoIncrementDetails(Column column, Database database, DatabaseSnapshot snapshot) {
        if (column.getAutoIncrementInformation() != null && database instanceof ExasolDatabase && database.getConnection() != null && !(database.getConnection() instanceof OfflineConnection)) {
            Map<String, Column.AutoIncrementInformation> autoIncrementColumns = (Map) snapshot.getScratchData("autoIncrementColumns");
            if (autoIncrementColumns == null) {
                autoIncrementColumns = new HashMap<String, Column.AutoIncrementInformation>();
                Executor executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor(EXECUTOR_NAME, database);
                try {
                    List<Map<String, ?>> rows = executor.queryForList(new RawSqlStatement("select object_schema_name(object_id) as schema_name, object_name(object_id) as table_name, name as column_name, cast(seed_value as bigint) as start_value, cast(increment_value as bigint) as increment_by from sys.identity_columns"));
                    for (Map row : rows) {
                        String schemaName = (String) row.get("SCHEMA_NAME");
                        String tableName = (String) row.get("TABLE_NAME");
                        String columnName = (String) row.get("COLUMN_NAME");
                        Long startValue = (Long) row.get("START_VALUE");
                        Long incrementBy = (Long) row.get("INCREMENT_BY");

                        Column.AutoIncrementInformation info = new Column.AutoIncrementInformation(startValue, incrementBy);
                        autoIncrementColumns.put(schemaName+"."+tableName+"."+columnName, info);
                    }
                    snapshot.setScratchData("autoIncrementColumns", autoIncrementColumns);
                } catch (DatabaseException e) {
                    Scope.getCurrentScope().getLog(getClass()).info("Could not read identity information", e);
                }
            }
            if (column.getRelation() != null && column.getSchema() != null) {
                Column.AutoIncrementInformation autoIncrementInformation = autoIncrementColumns.get(column.getSchema().getName() + "." + column.getRelation().getName() + "." + column.getName());
                if (autoIncrementInformation != null) {
                    column.setAutoIncrementInformation(autoIncrementInformation);
                }
            }
        }
    }

    protected DataType readDataType(CachedRow columnMetadataResultSet, Column column, Database database) throws SQLException {

        String columnTypeName = (String) columnMetadataResultSet.get("TYPE_NAME");


        DataType.ColumnSizeUnit columnSizeUnit = DataType.ColumnSizeUnit.BYTE;

        int dataType = columnMetadataResultSet.getInt("DATA_TYPE");
        Integer columnSize = null;
        Integer decimalDigits = null;
        if (!database.dataTypeIsNotModifiable(columnTypeName)) { // don't set size for types like int4, int8 etc
            columnSize = columnMetadataResultSet.getInt("COLUMN_SIZE");
            decimalDigits = columnMetadataResultSet.getInt("DECIMAL_DIGITS");
            if (decimalDigits != null && decimalDigits.equals(0)) {
                decimalDigits = null;
            }
        }

        Integer radix = columnMetadataResultSet.getInt("NUM_PREC_RADIX");

        Integer characterOctetLength = columnMetadataResultSet.getInt("CHAR_OCTET_LENGTH");


        DataType type = new DataType(columnTypeName);
        type.setDataTypeId(dataType);
        type.setColumnSize(columnSize);
        type.setDecimalDigits(decimalDigits);
        type.setRadix(radix);
        type.setCharacterOctetLength(characterOctetLength);
        type.setColumnSizeUnit(columnSizeUnit);

        return type;
    }

    protected Object readDefaultValue(CachedRow columnMetadataResultSet, Column columnInfo, Database database) throws SQLException, DatabaseException {

        return SqlUtil.parseValue(database, columnMetadataResultSet.get("COLUMN_DEF"), columnInfo.getType());
    }

}

