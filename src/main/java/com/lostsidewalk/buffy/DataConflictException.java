package com.lostsidewalk.buffy;

import java.io.Serial;
import java.util.Arrays;

/**
 * Custom exception class to represent data conflict errors in the application.
 * This exception is typically thrown when an attempt to insert or update data results in a conflict or duplication.
 */
public class DataConflictException extends Exception {

    @Serial
    private static final long serialVersionUID = 9823405923350982L;

    /**
     * Constructs a new DataConflictException with the specified error details.
     *
     * @param orgClassName The name of the originating class where the conflict occurred.
     * @param orgMethodName The name of the method within the originating class where the conflict occurred.
     * @param orgError A description of the conflict that occurred.
     * @param orgMethodArgs The arguments passed to the method where the conflict occurred.
     */
    public DataConflictException(String orgClassName, String orgMethodName, String orgError, Object... orgMethodArgs) {
        super("Data conflict error originated in " + orgClassName + "->" + orgMethodName +
                " (" + Arrays.toString(orgMethodArgs) + "): " + orgError);
    }
}
