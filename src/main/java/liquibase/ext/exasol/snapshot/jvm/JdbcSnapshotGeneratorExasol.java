package liquibase.ext.exasol.snapshot.jvm;

import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.Database;
import liquibase.database.core.InformixDatabase;
import liquibase.database.core.PostgresDatabase;
import liquibase.diff.DiffStatusListener;
import liquibase.exception.DatabaseException;
import liquibase.logging.LogFactory;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.InvalidExampleException;
import liquibase.snapshot.SnapshotGenerator;
import liquibase.snapshot.SnapshotGeneratorChain;
import liquibase.structure.DatabaseObject;

import java.util.HashSet;
import java.util.Set;

public abstract class JdbcSnapshotGeneratorExasol implements SnapshotGenerator {
    private Set<DiffStatusListener> statusListeners = new HashSet<DiffStatusListener>();

    private Class<? extends DatabaseObject> defaultFor = null;
    private Class<? extends DatabaseObject>[] addsTo = null;

    protected JdbcSnapshotGeneratorExasol(Class<? extends DatabaseObject> defaultFor) {
        this.defaultFor = defaultFor;
    }

    protected JdbcSnapshotGeneratorExasol(Class<? extends DatabaseObject> defaultFor, Class<? extends DatabaseObject>[] addsTo) {
        this.defaultFor = defaultFor;
        this.addsTo = addsTo;
    }

    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (database instanceof AbstractJdbcDatabase) {
            if (defaultFor != null && defaultFor.isAssignableFrom(objectType)) {
                return PRIORITY_DEFAULT;
            }
            if (addsTo() != null) {
                for (Class<? extends DatabaseObject> type : addsTo()) {
                    if (type.isAssignableFrom(objectType)) {
                        return PRIORITY_ADDITIONAL;
                    }
                }
            }
        }
        return PRIORITY_NONE;

    }

    @Override
    public Class<? extends DatabaseObject>[] addsTo() {
        return addsTo;
    }

    @Override
    public DatabaseObject snapshot(DatabaseObject example, DatabaseSnapshot snapshot, SnapshotGeneratorChain chain) throws DatabaseException, InvalidExampleException {

    System.err.println("jdbcSnapshotGenerator.snapshot - "
                             +"example=" + example.getObjectTypeName() + "." + example.getName()
                             + ", snapshot " + snapshot
                             + ")"
                        );

      
      if (defaultFor != null && defaultFor.isAssignableFrom(example.getClass())) {
        System.err.println("jdbcSnapshotGenerator.snapshot - defaultFor tests passed "
                                 + ": returning snapshotObject(example, snapshot)"
                            );
            return snapshotObject(example, snapshot);
        }

        DatabaseObject chainResponse = chain.snapshot(example, snapshot);
        if (chainResponse == null) {
          System.err.println("jdbcSnapshotGenerator.snapshot -  chainResponse is null - RETURNING"
                              );
            return null;
        }

        if (shouldAddTo(example.getClass(), snapshot)) {
          System.err.println("jdbcSnapshotGenerator.snapshot -  shouldAddTo");
          if (addsTo() != null) {
            System.err.println("jdbcSnapshotGenerator.snapshot -  addsTo() != null");

            for (Class<? extends DatabaseObject> addType : addsTo()) {
                    if (addType.isAssignableFrom(example.getClass())) {
                        if (chainResponse != null) {
                           System.err.println("jdbcSnapshotGenerator.snapshot -  addsTo(chainResponse, snapshot)");

                          addTo(chainResponse, snapshot);
                        }
                    }
                }
            }
        }

        System.err.println("jdbcSnapshotGenerator.snapshot - "
                             +"RETURNING chainresponse=" + chainResponse.getName()
                             + ")"
                        );

        return chainResponse;

    }

    protected boolean shouldAddTo(Class<? extends DatabaseObject> databaseObjectType, DatabaseSnapshot snapshot) {
        return defaultFor != null && snapshot.getSnapshotControl().shouldInclude(defaultFor);
    }

    @Override
    public Class<? extends SnapshotGenerator>[] replaces() {
        return null;
    }

    protected abstract DatabaseObject snapshotObject(DatabaseObject example, DatabaseSnapshot snapshot) throws DatabaseException, InvalidExampleException;

    protected abstract void addTo(DatabaseObject foundObject, DatabaseSnapshot snapshot) throws DatabaseException, InvalidExampleException;

//    public Boolean has(DatabaseObject example, DatabaseSnapshot snapshot, SnapshotGeneratorChain chain) throws DatabaseException {
//        return null;  //To change body of implemented methods use File | Settings | File Templates.
//    }

    public void addStatusListener(DiffStatusListener listener) {
        statusListeners.add(listener);
    }

    protected void updateListeners(String message) {
        if (this.statusListeners == null) {
            return;
        }
        LogFactory.getLogger().debug(message);
        for (DiffStatusListener listener : this.statusListeners) {
            listener.statusUpdate(message);
        }
    }

    protected String cleanNameFromDatabase(String objectName, Database database) {
        if (objectName == null) {
            return null;
        }
        return objectName;
    }
}
