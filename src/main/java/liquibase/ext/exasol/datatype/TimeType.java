package liquibase.ext.exasol.datatype;

import liquibase.database.Database;
import liquibase.datatype.DatabaseDataType;
import liquibase.statement.DatabaseFunction;
import liquibase.ext.exasol.database.ExasolDatabase;


import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;

public class TimeType extends liquibase.datatype.core.TimeType {
    
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

    @Override
    public Object sqlToObject(String value, Database database) {
        if (zeroTime(value)) {
            return value;
        }

        try {
            DateFormat timeFormat = getTimeFormat(database);

            if (value.matches("to_timestamp\\('\\d+:\\d+:\\d+', 'HH24:MI:SS.FF9'\\)")) {
                timeFormat = new SimpleDateFormat("HH:mm:s");
                value = value.replaceFirst(".*?'", "").replaceFirst("',.*","");
            }

            return new java.sql.Time(timeFormat.parse(value).getTime());
        } catch (ParseException e) {
            return new DatabaseFunction(value);
        }
    }

    /**
     * Returns the value object in a format to include in SQL. Quote if necessary.
     */
    @Override
    public String objectToSql(Object value, Database database) {
        if (value == null || value.toString().equalsIgnoreCase("null")) {
            return null;
        } else if (value instanceof DatabaseFunction) {
            return functionToSql((DatabaseFunction) value, database);
        } else if (value instanceof java.sql.Time) {
            return database.getTimeLiteral(((java.sql.Time) value));
        }
        return otherToSql(value, database);
    }


    private boolean zeroTime(String stringVal) {
        return stringVal.replace("-","").replace(":", "").replace(" ","").replace("0","").equals("");
    }

    @Override
    protected DateFormat getTimeFormat(Database database) {
        return new SimpleDateFormat("HH:mm:ss.SSS");
    }


}
