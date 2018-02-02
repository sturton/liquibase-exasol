package liquibase.ext.exasol.database;

import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class ExasolDatabaseTest {

    @Test
    public void getShortName() {
        assertEquals("exasol", new ExasolDatabase().getShortName());
    }
}
