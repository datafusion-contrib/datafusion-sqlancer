package sqlancer.datafusion;

import static sqlancer.datafusion.DataFusionUtil.dfAssert;

import sqlancer.common.query.ExpectedErrors;

public final class DataFusionErrors {
    private DataFusionErrors() {
        dfAssert(false, "Utility class cannot be instantiated");
    }

    /*
     * During Oracle Checks, if ANY query returns one of the following error Then
     * the current oracle check will be
     * skipped. e.g.: NoREC Q1 -> throw an expected error NoREC Q2 -> succeed Since
     * it's a known error, `SQLancer` will
     * skip this check and don't report bug.
     *
     * Note now it's implemented this way for simplicity This way might cause false
     * negative, because Q1 and Q2 should
     * both succeed or both fail TODO(datafusion): ensure both succeed or both fail
     */
    public static void registerExpectedExecutionErrors(ExpectedErrors errors) {
        /*
         * Expected
         */
        errors.add("Error building plan"); // Randomly generated SQL is not valid and caused palning error
        errors.add("Error during planning");
        errors.add("Execution error");
        errors.add("Overflow happened");
        errors.add("overflow");
        errors.add("Unsupported data type");
        errors.add("Divide by zero");
        errors.add("Sort requires at least one column");
        errors.add("The data type type Null has no natural order");
        errors.add("Regular expression did not compile");
        errors.add("Cannot cast value");
        errors.add("regex parse error");
        errors.add("Invalid string operation: List"); // select [1,2] like null;
        errors.add("Unsupported CAST from List"); // not sure
        errors.add("This feature is not implemented: Support for 'approx_distinct' for data type");
        errors.add("MedianAccumulator not supported for median");
        errors.add("Percentile value for 'APPROX_PERCENTILE_CONT' must be Float32 or Float64 literal");
        errors.add("digest max_size value for 'APPROX_PERCENTILE_CONT' must be UInt > 0 literal ");
        errors.add("Aggregate can not be used as a sliding accumulator because `retract_batch` is not implemented");
        errors.add("Arrow error: Invalid argument error: Invalid arithmetic operation: Utf8");
        errors.add("There is only support Literal types for field at idx:");
        errors.add("nth_value not supported for n:");
        errors.add("Invalid argument error: Nested comparison: List(");
        errors.add("This feature is not implemented: Percentile value for 'APPROX_PERCENTILE_CONT' must be a literal");
        errors.add(
                "This feature is not implemented: Tdigest max_size value for 'APPROX_PERCENTILE_CONT' must be a literal");

        /*
         * Known bugs
         */
        errors.add("bitwise"); // https://github.com/apache/datafusion/issues/11260
        errors.add("Sort expressions cannot be empty for streaming merge."); // https://github.com/apache/datafusion/issues/11561
        errors.add("Schema error: No field named "); // https://github.com/apache/datafusion/issues/12006
        // errors.add("regexp_is_match"); //
        // https://github.com/apache/datafusion/issues/12180
        errors.add("SUBSTR"); // https://github.com/apache/datafusion/issues/12699

        /*
         * False positives
         */
        errors.add("Cannot cast string"); // ifnull() is passed two non-compattable type and caused execution error
        // False positive: when aggr is generated in where clause
        errors.add("Physical plan does not support logical expression AggregateFunction");
        errors.add("Unsupported ArrowType Utf8View"); // Maybe bug in arrow flight
        // jdbc driver

        /*
         * Not critical, investigate in the future
         */
        errors.add("does not match with the projection expression");
        errors.add("invalid operator for nested");
        errors.add("Arrow error: Cast error: Can't cast value");
        errors.add("Nth value indices are 1 based");
        errors.add("could not cast value to arrow_array:"); // https://github.com/apache/datafusion/issues/12150
        errors.add("length not supported for Utf8View"); // https://github.com/apache/datafusion/issues/12149
    }
}
