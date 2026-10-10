package com.qqmu.jync.service.cdc;

import lombok.Getter;

/**
 * A binlog value this tool refuses to guess at (task book B5: unmappable types are
 * logged as ERROR and counted — never silently dropped or half-converted). The message
 * carries column, type name and the offending value so one log line is enough to
 * diagnose the skip.
 */
@Getter
public class CdcUnmappableValueException extends Exception {

    private final String column;
    private final String typeName;

    public CdcUnmappableValueException(String column, String typeName, Object value) {
        super("Cannot map binlog value for column '" + column + "' (type " + typeName
                + ", java " + (value == null ? "null" : value.getClass().getName())
                + ", value " + render(value) + "); row skipped and counted");
        this.column = column;
        this.typeName = typeName;
    }

    private static String render(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof byte[]) {
            return "byte[" + ((byte[]) value).length + "]";
        }
        return String.valueOf(value);
    }
}
