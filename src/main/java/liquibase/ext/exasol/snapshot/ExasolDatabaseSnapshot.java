package liquibase.ext.exasol.snapshot;

import liquibase.CatalogAndSchema;
import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.Database;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.ext.exasol.database.ExasolDatabase;
import liquibase.snapshot.CachedRow;
import liquibase.snapshot.InvalidExampleException;
import liquibase.snapshot.JdbcDatabaseSnapshot;
import liquibase.snapshot.SnapshotControl;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Schema;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Cribbed from liquibase-verticalDatabase.
 */
public class ExasolDatabaseSnapshot extends JdbcDatabaseSnapshot {
    ExasolCachingDatabaseMetaData exasolCachingDatabaseMetaData;
    ResultSetCache tablesResultCache;
    public ExasolDatabaseSnapshot(DatabaseObject[] examples, Database database, SnapshotControl snapshotControl) throws DatabaseException, InvalidExampleException {
        super(examples, database, snapshotControl);
        tablesResultCache = new ResultSetCache();

    }

    public ExasolDatabaseSnapshot(DatabaseObject[] examples, Database database) throws DatabaseException, InvalidExampleException {
        super(examples, database);
        tablesResultCache = new ResultSetCache();
    }


    public ExasolCachingDatabaseMetaData getMetaData() throws SQLException {
        if (exasolCachingDatabaseMetaData == null) {
            DatabaseMetaData databaseMetaData = null;
            if (getDatabase().getConnection() != null) {
                databaseMetaData = ((JdbcConnection) getDatabase().getConnection()).getUnderlyingConnection().getMetaData();
            }
            exasolCachingDatabaseMetaData = new ExasolCachingDatabaseMetaData(this.getDatabase(), databaseMetaData);
        }
        return exasolCachingDatabaseMetaData;
    }


    public class ExasolCachingDatabaseMetaData extends JdbcDatabaseSnapshot.CachingDatabaseMetaData {
        private DatabaseMetaData databaseMetaData;
        private Database database;

        public ExasolCachingDatabaseMetaData(Database database, DatabaseMetaData metaData) {
            super(database, metaData);
            this.databaseMetaData = metaData;
            this.database = database;
        }


        public List<CachedRow> getTables(final String schemaName, final String table) throws SQLException, DatabaseException {
            return tablesResultCache.get(new ResultSetCache.SingleResultSetExtractor(database){

                @Override
                public ResultSetCache.RowData rowKeyParameters(CachedRow row) {
                    return new ResultSetCache.RowData(row.getString("TABLE_CAT"), row.getString("TABLE_SCHEM"), database, row.getString("TABLE_NAME"));
                }

                @Override
                public ResultSetCache.RowData wantedKeyParameters() {
                    return new ResultSetCache.RowData(null, schemaName, database, table);
                }

                @Override
                public List<CachedRow> fastFetchQuery() throws SQLException, DatabaseException {
                    CatalogAndSchema catalogAndSchema = new CatalogAndSchema(null, schemaName).customize(database);


                    String catalog = ((AbstractJdbcDatabase) database).getJdbcCatalogName(catalogAndSchema);
                    String schema = ((AbstractJdbcDatabase) database).getJdbcSchemaName(catalogAndSchema);
                    return extract(databaseMetaData.getTables(catalog, schema, table, new String[]{"TABLE"}));
                }

                @Override
                public List<CachedRow> bulkFetchQuery() throws SQLException, DatabaseException {
                    CatalogAndSchema catalogAndSchema = new CatalogAndSchema(null, schemaName).customize(database);

                    String catalog = ((AbstractJdbcDatabase) database).getJdbcCatalogName(catalogAndSchema);
                    String schema = ((AbstractJdbcDatabase) database).getJdbcSchemaName(catalogAndSchema);
                    return extract(databaseMetaData.getTables(catalog, schema, null, new String[]{"TABLE"}));
                }
            });
        }
    }
}

