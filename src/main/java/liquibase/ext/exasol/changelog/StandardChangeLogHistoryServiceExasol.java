package liquibase.ext.exasol.changelog;

import liquibase.changelog.StandardChangeLogHistoryService;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.ext.exasol.database.ExasolDatabase;
import liquibase.servicelocator.PrioritizedService;
import liquibase.snapshot.SnapshotGeneratorFactory;
import liquibase.snapshot.jvm.ColumnSnapshotGenerator;

public class StandardChangeLogHistoryServiceExasol extends StandardChangeLogHistoryService {

    @Override
    public boolean supports(Database database) {
        return database instanceof ExasolDatabase;
    }

    @Override
    public int getPriority() {
        return PrioritizedService.PRIORITY_DATABASE;
    }

    @Override
    public void init() throws DatabaseException {
        //@TODO - can we replace this using SnapshotGenerate replaces() method?
        SnapshotGeneratorFactory.getInstance().unregister(ColumnSnapshotGenerator.class);
        super.init();
    }
}


