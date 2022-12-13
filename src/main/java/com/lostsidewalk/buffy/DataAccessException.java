package com.lostsidewalk.buffy;

import java.util.Arrays;

public class DataAccessException extends Exception {

    // this is caused when an exception occurs during JDBC invocation/parameter preparation , e.g. loss of DB connection, etc.
    public DataAccessException(String orgClassName, String orgMethodName, String orgError, Object... orgMethodArgs) {
        super("Data access error originated in " + orgClassName + "->" + orgMethodName + " (" + Arrays.toString(orgMethodArgs) + "): " + orgError);
    }
}
