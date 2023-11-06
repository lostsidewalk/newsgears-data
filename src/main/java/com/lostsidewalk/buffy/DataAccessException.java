package com.lostsidewalk.buffy;

import java.io.Serial;
import java.util.Arrays;

/**
 * Custom exception class to represent data access errors in the application.
 * This exception is typically thrown when an error occurs during database or data retrieval operations.
 */
public class DataAccessException extends Exception {

    @Serial
    private static final long serialVersionUID = 982340598623450982L;

    /**
     * Constructs a new DataAccessException with the specified error details.
     *
     * @param orgClassName The name of the originating class where the error occurred.
     * @param orgMethodName The name of the method within the originating class where the error occurred.
     * @param orgError A description of the error that occurred.
     * @param orgMethodArgs The arguments passed to the method where the error occurred.
     */
    public DataAccessException(String orgClassName, String orgMethodName, String orgError, Object... orgMethodArgs) {
        super("Data access error originated in " + orgClassName + "->" + orgMethodName +
                " (" + Arrays.toString(orgMethodArgs) + "): " + orgError);
    }
}
