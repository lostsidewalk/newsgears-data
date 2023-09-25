package com.lostsidewalk.buffy;

import java.util.Arrays;

/**
 * Exception class representing errors related to data update operations.
 */
public class DataUpdateException extends Exception {

    /**
     * Constructs a new DataUpdateException with information about the origin of the error.
     *
     * @param orgClassName  The name of the originating class where the error occurred.
     * @param orgMethodName The name of the originating method where the error occurred.
     * @param orgMethodArgs The arguments (if any) of the originating method where the error occurred.
     */
    public DataUpdateException(String orgClassName, String orgMethodName, Object... orgMethodArgs) {
        super("Data update error originated in " + orgClassName + "->" + orgMethodName + " (" + Arrays.toString(orgMethodArgs) + ")");
    }
}
