package sqlancer.datafusion.test;

import static sqlancer.datafusion.DataFusionUtil.DataFusionLogger.DataFusionLogType.ERROR;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.datafusion.DataFusionErrors;
import sqlancer.datafusion.DataFusionProvider.DataFusionGlobalState;
import sqlancer.datafusion.DataFusionSchema;
import sqlancer.datafusion.DataFusionToStringVisitor;
import sqlancer.datafusion.DataFusionUtil;
import sqlancer.datafusion.ast.DataFusionExpression;
import sqlancer.datafusion.ast.DataFusionSelect;

public class DataFusionQueryPartitioningAggrTester extends DataFusionQueryPartitioningBase {
    public DataFusionQueryPartitioningAggrTester(DataFusionGlobalState state) {
        super(state);
        DataFusionErrors.registerExpectedExecutionErrors(errors);
    }

    /*
     * Query Partitioning - Aggregate
     *
     * q: SELECT min([expr1]) FROM [expr2]
     *
     * qp1: SELECT min([expr1]) FROM [expr2] WHERE [expr3]
     *
     * qp2: SELECT min([expr1]) FROM [expr2] WHERE NOT [expr3]
     *
     * qp3: SELECT min([expr1]) FROM [expr2] WHERE [expr3] IS NULL
     *
     * Oracle check: q's result equals to min(qp1, qp2, qp3)
     */
    @Override
    public void check() throws SQLException {
        // generate a random 'SELECT [expr1] FROM [expr2] WHERE [expr3]
        super.check();

        DFAggrOp aggrOp = Randomly.fromOptions(DFAggrOp.values());
        checkAggregate(aggrOp);
    }

    void checkAggregate(DFAggrOp aggrOP) throws SQLException {
        DataFusionSelect randomSelect = select;

        String qString = "";
        String qp1String = "";
        String qp2String = "";
        String qp3String = "";

        randomSelect.setWhereClause(null);
        Node<DataFusionExpression> fetchExpr = randomSelect.exprGenAll
                .generateExpression(DataFusionSchema.DataFusionDataType.getRandomWithoutNull()); // e.g. col1 + col2
        String fetchString = aggrOP.name() + "(" + DataFusionToStringVisitor.asString(fetchExpr) + ")";
        // after: MIN(col1 + col2)

        randomSelect.setFetchColumnsString(fetchString);
        qString = DataFusionToStringVisitor.asString(randomSelect);

        randomSelect.setWhereClause(predicate);
        qp1String = DataFusionToStringVisitor.asString(randomSelect);

        randomSelect.setWhereClause(negatedPredicate);
        qp2String = DataFusionToStringVisitor.asString(randomSelect);

        randomSelect.setWhereClause(isNullPredicate);
        qp3String = DataFusionToStringVisitor.asString(randomSelect);

        // q - min(q1, q2, q3)
        String diffQuery = aggrOP.formatDiffQuery(Arrays.asList(qString, qp1String, qp2String, qp3String));

        List<String> diffQueryResultSet = null;
        try {
            diffQueryResultSet = ComparatorHelper.getResultSetFirstColumnAsString(diffQuery, errors, state);
        } catch (AssertionError e) {
            // Append more error message
            String replay = DataFusionUtil.getReplay(state.getDatabaseName());
            String newMessage = e.getMessage() + "\n" + e.getCause() + "\n" + replay + "\n";
            state.dfLogger.appendToLog(ERROR, newMessage);

            throw new AssertionError(newMessage);
        }

        String diffResultString = diffQueryResultSet != null ? diffQueryResultSet.get(0) : "Query Failed";
        // inf - inf
        if (diffResultString == null || diffResultString.equals("NaN")
                || diffResultString.toLowerCase().contains("inf")) {
            return;
        }
        double diff = -1;
        try {
            diff = Double.parseDouble(diffResultString);
        } catch (Exception e) {
        }

        // TODO(datafusion) remove 1e100 condition when overflow is solved
        // https://github.com/apache/datafusion/issues/3520
        if (Math.abs(diff) > 1e-3 && Math.abs(diff) < 1e100) {
            StringBuilder errorMessage = new StringBuilder().append("TLP-Aggregate oracle violated:\n")
                    .append(aggrOP.errorReportDescription()).append(diffResultString).append("\n").append("Q: ")
                    .append(qString).append("\n").append("Q1: ").append(qp1String).append("\n").append("Q2: ")
                    .append(qp2String).append("\n").append("Q3: ").append(qp3String).append("\n").append(diffQuery)
                    .append("\n").append("=======================================\n").append("Reproducer: \n");

            String replay = DataFusionUtil.getReplay(state.getDatabaseName());

            String errorLog = errorMessage.toString() + replay + "\n";
            String indentedErrorLog = errorLog.replaceAll("(?m)^", "    ");
            state.dfLogger.appendToLog(ERROR, errorLog);

            throw new AssertionError("\n\n" + indentedErrorLog);
        }
    }

    private interface DataFusionTLPAggregate {
        // e.g. q - min(q1, q2, q3), in the form of single SQL query
        // Oracle will check diff query equals to 0

        // Accepts a list of strings with expected order: q, q1, q2, q3
        // to make linter happy :)
        String formatDiffQuery(List<String> queries);

        String errorReportDescription();
    }

    private enum DFAggrOp implements DataFusionTLPAggregate {
        MIN {
            @Override
            public String formatDiffQuery(List<String> queries) {
                String q = queries.get(0);
                String q1 = queries.get(1);
                String q2 = queries.get(2);
                String q3 = queries.get(3);

                return "SELECT " + "(" + q + ") - " + "(" + "    SELECT MIN(value) " + "    FROM ("
                        + "        SELECT (" + q1 + ") AS value " + "        UNION ALL " + "        SELECT (" + q2
                        + ") " + "        UNION ALL " + "        SELECT (" + q3 + ") " + "    ) AS sub"
                        + ") AS result_difference;";
            }

            @Override
            public String errorReportDescription() {
                return "Q's result is not equalt to MIN(Q1, Q2, Q3): RS(Q) - MIN(RS(Q1), RS(Q2), RS(Q3)) is :";
            }
        },
        MAX {
            @Override
            public String formatDiffQuery(List<String> queries) {
                String q = queries.get(0);
                String q1 = queries.get(1);
                String q2 = queries.get(2);
                String q3 = queries.get(3);

                return "SELECT " + "(" + q + ") - " + "(" + "    SELECT MAX(value) " + "    FROM ("
                        + "        SELECT (" + q1 + ") AS value " + "        UNION ALL " + "        SELECT (" + q2
                        + ") " + "        UNION ALL " + "        SELECT (" + q3 + ") " + "    ) AS sub"
                        + ") AS result_difference;";
            }

            @Override
            public String errorReportDescription() {
                return "Q's result is not equalt to MAX(Q1, Q2, Q3): RS(Q) - MAX(RS(Q1), RS(Q2), RS(Q3)) is :";
            }
        },
        COUNT {
            @Override
            public String formatDiffQuery(List<String> queries) {
                String q = queries.get(0);
                String q1 = queries.get(1);
                String q2 = queries.get(2);
                String q3 = queries.get(3);

                return "SELECT " + "(" + q + ") - " + "(" + "    SELECT SUM(value) " + "    FROM ("
                        + "        SELECT (" + q1 + ") AS value " + "        UNION ALL " + "        SELECT (" + q2
                        + ") " + "        UNION ALL " + "        SELECT (" + q3 + ") " + "    ) AS sub"
                        + ") AS result_difference;";
            }

            @Override
            public String errorReportDescription() {
                return "Q's result is not equalt to SUM(Q1, Q2, Q3): RS(Q) - SUM(RS(Q1), RS(Q2), RS(Q3)) is :";
            }
        },
        SUM {
            @Override
            public String formatDiffQuery(List<String> queries) {
                String q = queries.get(0);
                String q1 = queries.get(1);
                String q2 = queries.get(2);
                String q3 = queries.get(3);

                return "SELECT " + "(" + q + ") - " + "(" + "    SELECT SUM(value) " + "    FROM ("
                        + "        SELECT (" + q1 + ") AS value " + "        UNION ALL " + "        SELECT (" + q2
                        + ") " + "        UNION ALL " + "        SELECT (" + q3 + ") " + "    ) AS sub"
                        + ") AS result_difference;";
            }

            @Override
            public String errorReportDescription() {
                return "Q's result is not equalt to SUM(Q1, Q2, Q3): RS(Q) - SUM(RS(Q1), RS(Q2), RS(Q3)) is :";
            }
        };
    }

}
