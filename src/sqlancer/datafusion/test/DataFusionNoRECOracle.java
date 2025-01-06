package sqlancer.datafusion.test;

import static sqlancer.datafusion.DataFusionUtil.DataFusionLogger.DataFusionLogType.ERROR;
import static sqlancer.datafusion.ast.DataFusionSelect.getRandomSelect;

import java.sql.SQLException;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.IgnoreMeException;
import sqlancer.common.oracle.NoRECBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.datafusion.DataFusionErrors;
import sqlancer.datafusion.DataFusionProvider.DataFusionGlobalState;
import sqlancer.datafusion.DataFusionToStringVisitor;
import sqlancer.datafusion.DataFusionUtil;
import sqlancer.datafusion.ast.DataFusionSelect;

public class DataFusionNoRECOracle extends NoRECBase<DataFusionGlobalState>
        implements TestOracle<DataFusionGlobalState> {

    private final DataFusionGlobalState state;

    public DataFusionNoRECOracle(DataFusionGlobalState globalState) {
        super(globalState);
        this.state = globalState;
        DataFusionErrors.registerExpectedExecutionErrors(errors);
    }

    /*
     * Non-Optimizing Reference Engine Construction
     *
     * q1: SELECT [expr1] FROM [expr2] WHERE [expr3]
     *
     * q2: SELECT [expr3] FROM [expr2]
     *
     * Oracle Check: q1's result size equals to `true` count in q2's result set
     */
    @Override
    public void check() throws SQLException {
        /*
         * Setup Q1 and Q2
         */
        // generate a random:
        // SELECT [expr1] FROM [expr2] WHERE [expr3]
        DataFusionSelect randomSelect = getRandomSelect(state);

        // Q1: SELECT count(*) FROM [expr2] WHERE [expr3]
        // Q1 and Q2 is constructed from randomSelect's fields
        // So for equivalent table mutation, we mutate randomSelect
        randomSelect.mutateEquivalentTableName();
        DataFusionSelect q1 = new DataFusionSelect();
        q1.setFetchColumnsString("COUNT(*)");
        q1.from = randomSelect.from;
        q1.setWhereClause(randomSelect.getWhereClause());
        String q1String = DataFusionToStringVisitor.asString(q1);

        // Q2: SELECT count(case when [expr3] then 1 else null end) FROM [expr2]
        randomSelect.mutateEquivalentTableName();
        DataFusionSelect q2 = new DataFusionSelect();
        String selectExpr = String.format("COUNT(CASE WHEN %s THEN 1 ELSE NULL END)",
                DataFusionToStringVisitor.asString(randomSelect.getWhereClause()));
        q2.setFetchColumnsString(selectExpr);
        q2.from = randomSelect.from;
        q2.setWhereClause(null);
        String q2String = DataFusionToStringVisitor.asString(q2);

        /*
         * Execute Q1 and Q2
         */
        // System.out.println("DBG: " + q1String + "\n" + q2String);
        List<String> q1ResultSet = null;
        List<String> q2ResultSet = null;
        try {
            q1ResultSet = ComparatorHelper.getResultSetFirstColumnAsString(q1String, errors, state);
            q2ResultSet = ComparatorHelper.getResultSetFirstColumnAsString(q2String, errors, state);
        } catch (AssertionError e) {
            // Append detailed error message
            String replay = DataFusionUtil.getReplay(state.getDatabaseName());
            String newMessage = e.getMessage() + "\n" + e.getCause() + "\n" + replay + "\n";
            state.dfLogger.appendToLog(ERROR, newMessage);

            throw new AssertionError(newMessage);
        }

        /*
         * NoREC check
         */
        int count1 = q1ResultSet != null ? Integer.parseInt(q1ResultSet.get(0)) : -1;
        int count2 = q2ResultSet != null ? Integer.parseInt(q2ResultSet.get(0)) : -1;
        if (count1 != count2) {
            // whitelist
            // ---------
            // https://github.com/apache/datafusion/issues/12468
            if (q1String.contains("NATURAL JOIN")) {
                throw new IgnoreMeException();
            }

            StringBuilder errorMessage = new StringBuilder().append("NoREC oracle violated:\n")
                    .append("    Q1(result size ").append(count1).append("):").append(q1String).append(";\n")
                    .append("    Q2(result size ").append(count2).append("):").append(q2String).append(";\n")
                    .append("=======================================\n").append("Reproducer: \n");

            String replay = DataFusionUtil.getReplay(state.getDatabaseName());

            String errorLog = errorMessage.toString() + replay + "\n";
            String indentedErrorLog = errorLog.replaceAll("(?m)^", "    ");
            state.dfLogger.appendToLog(ERROR, errorLog);

            throw new AssertionError("\n\n" + indentedErrorLog);
        }
        // System.out.println("NOREC passed: \n" + q1String + "\n" + q2String);
    }
}
