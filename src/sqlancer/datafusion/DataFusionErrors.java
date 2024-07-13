package sqlancer.datafusion;

import static sqlancer.datafusion.DataFusionUtil.dfAssert;

import java.util.regex.Pattern;

import sqlancer.common.query.ExpectedErrors;

public final class DataFusionErrors {
    private DataFusionErrors() {
        dfAssert(false, "Utility class cannot be instantiated");
    }

    /*
     * During Oracle Checks, if ANY query returns one of the following error Then the current oracle check will be
     * skipped. e.g.: NoREC Q1 -> throw an expected error NoREC Q2 -> succeed Since it's a known error, `SQLancer` will
     * skip this check and don't report bug.
     *
     * Note now it's implemented this way for simplicity This way might cause false negative, because Q1 and Q2 should
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
        /*
         * Known bugs
         */
        errors.add("to type Int64"); // https://github.com/apache/datafusion/issues/11249
        errors.add("bitwise"); // https://github.com/apache/datafusion/issues/11260
        errors.add(" Not all InterleaveExec children have a consistent hash partitioning."); // https://github.com/apache/datafusion/issues/11409
        Pattern pattern = Pattern.compile("JOIN.*NULL", Pattern.CASE_INSENSITIVE);
        errors.addRegex(pattern); // https://github.com/apache/datafusion/issues/11414
        /*
         * False positives
         */
        errors.add("Cannot cast string"); // ifnull() is passed two non-compattable type and caused execution error
        errors.add("Physical plan does not support logical expression AggregateFunction"); // False positive: when aggr
        // is generated in where
        // clause
    }
}
