package com.lostsidewalk.buffy;

import java.util.Arrays;

public class DataUpdateException extends Exception {

    // this is caused when a DB write results in 0 rows updated, e.g. data missing/wrong ID supplied to an update statement, etc.
    public DataUpdateException(String orgClassName, String orgMethodname, Object... orgMethodArgs) {
        super("Data update error originated in " + orgClassName + "->" + orgMethodname + " (" + Arrays.toString(orgMethodArgs) + ")");
    }
}
