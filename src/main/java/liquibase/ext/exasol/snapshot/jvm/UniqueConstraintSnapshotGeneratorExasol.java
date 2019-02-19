package liquibase.ext.exasol.snapshot.jvm;

import liquibase.database.Database;
import liquibase.database.core.*;
import liquibase.exception.DatabaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.executor.ExecutorService;
import liquibase.snapshot.*;
import liquibase.snapshot.jvm.JdbcSnapshotGenerator ;
import liquibase.snapshot.jvm.UniqueConstraintSnapshotGenerator ;
import liquibase.statement.core.RawSqlStatement;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.*;
import liquibase.util.StringUtils;

import java.sql.SQLException;
import java.util.*;

import liquibase.ext.exasol.database.ExasolDatabase;

/**
 * Placeholder class to prevent default search for UNIQUE constraints, which Exasol does not currently support (Exasol 6.1).
 **/
public class UniqueConstraintSnapshotGeneratorExasol extends JdbcSnapshotGenerator {

    public UniqueConstraintSnapshotGeneratorExasol() {
        super(UniqueConstraint.class, new Class[]{Table.class});
    }

    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (database instanceof ExasolDatabase) {
            return PRIORITY_ADDITIONAL;
        }
	return PRIORITY_NONE;
    }

    @Override
    public Class<? extends SnapshotGenerator>[] replaces() {
        Class<? extends SnapshotGenerator>[] replacedSnapshotGenerators  = ( Class<? extends SnapshotGenerator>[]) new Class[1] ;
       	replacedSnapshotGenerators[0] = UniqueConstraintSnapshotGenerator.class ;
        return replacedSnapshotGenerators ; //null; //new Class<? extends SnapshotGenerator>[] = { UniqueConstraintSnapshotGenerator.class} ;
    }


    @Override
    protected DatabaseObject snapshotObject(DatabaseObject example, DatabaseSnapshot snapshot) throws DatabaseException, InvalidExampleException {
        Database database = snapshot.getDatabase();

	//Exasol does not support UNIQUE constraints 
	return null;
    }

    @Override
    protected void addTo(DatabaseObject foundObject, DatabaseSnapshot snapshot) throws DatabaseException, InvalidExampleException {

	return;
    }

    protected List<CachedRow> listConstraints(Table table, DatabaseSnapshot snapshot, Schema schema) throws DatabaseException, SQLException {
	List<CachedRow> returnList = new ArrayList<CachedRow>();
	return returnList;
    }

    protected List<Map<String, ?>> listColumns(UniqueConstraint example, Database database) throws DatabaseException {
        Table table = example.getTable();
        Schema schema = table.getSchema();
        String name = example.getName();

	List<Map<String, ?>> returnList = new ArrayList<Map<String, ?>>();
	return returnList;
    }

}
