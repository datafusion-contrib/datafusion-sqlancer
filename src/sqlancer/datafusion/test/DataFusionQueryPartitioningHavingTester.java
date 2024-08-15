package sqlancer.datafusion.test;

import static sqlancer.datafusion.DataFusionUtil.DataFusionLogger.DataFusionLogType.ERROR;
import static sqlancer.datafusion.gen.DataFusionExpressionGenerator.generateHavingClause;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.datafusion.DataFusionErrors;
import sqlancer.datafusion.DataFusionProvider.DataFusionGlobalState;
import sqlancer.datafusion.DataFusionToStringVisitor;
import sqlancer.datafusion.DataFusionUtil;
import sqlancer.datafusion.ast.DataFusionExpression;
import sqlancer.datafusion.ast.DataFusionSelect;
import sqlancer.datafusion.gen.DataFusionExpressionGenerator;

public class DataFusionQueryPartitioningHavingTester extends DataFusionQueryPartitioningBase {
    public DataFusionQueryPartitioningHavingTester(DataFusionGlobalState state) {
        super(state);
        DataFusionErrors.registerExpectedExecutionErrors(errors);
    }

    /*
     * Query Partitioning - Where
     *
     * q: SELECT [expr1] FROM [expr2]
     *
     * qp1: SELECT [expr1] FROM [expr2] HAVING [expr3]
     *
     * qp2: SELECT [expr1] FROM [expr2] HAVING NOT [expr3]
     *
     * qp3: SELECT [expr1] FROM [expr2] HAVING [expr3] IS NULL
     *
     * Oracle check: q's result equals to union(qp1, qp2, qp3)
     */
    @Override
    public void check() throws SQLException {
        // generate a random 'SELECT [expr1] FROM [expr2] WHERE [expr3]
        super.check();
        DataFusionSelect randomSelect = select;

        if (Randomly.getBoolean()) {
            if (Randomly.getBoolean()) {
                randomSelect.distinct = true;
            }

            if (Randomly.getBoolean()) {
                randomSelect.setOrderByClauses(gen.generateOrderBys());
            }
        }

        if (Randomly.getBoolean()) {
            randomSelect.setWhereClause(gen.generatePredicate());
        }

        // generate {group_by_cols, aggrs}
        if (!Randomly.getBooleanWithSmallProbability()) {
            randomSelect.setAggregates(state);
        }

        // DataFusionExpressionGenerator havingGen = randomSelect.exprGenAggregate;
        DataFusionExpressionGenerator groupByGen = randomSelect.exprGenGroupBy;
        Node<DataFusionExpression> havingPredicate = generateHavingClause(randomSelect.exprGenGroupBy,
                randomSelect.exprGenAggregate);
        Node<DataFusionExpression> negateHavingPredicate = groupByGen.negatePredicate(havingPredicate);
        Node<DataFusionExpression> isNullHavingPredicate = groupByGen.isNull(havingPredicate);

        String qString = "";
        String qp1String = "";
        String qp2String = "";
        String qp3String = "";
        randomSelect.setHavingClause(null);
        qString = DataFusionToStringVisitor.asString(randomSelect);

        randomSelect.setHavingClause(havingPredicate);
        qp1String = DataFusionToStringVisitor.asString(randomSelect);

        randomSelect.setHavingClause(negateHavingPredicate);
        qp2String = DataFusionToStringVisitor.asString(randomSelect);

        randomSelect.setHavingClause(isNullHavingPredicate);
        qp3String = DataFusionToStringVisitor.asString(randomSelect);

        List<String> qResultSet = new ArrayList<>();
        List<String> qpResultSet = new ArrayList<>();
        try {
            /*
             * Run all queires
             */
            qResultSet = ComparatorHelper.getResultSetFirstColumnAsString(qString, errors, state);
            List<String> combinedString = new ArrayList<>();
            qpResultSet = ComparatorHelper.getCombinedResultSet(qp1String, qp2String, qp3String, combinedString, true,
                    state, errors);
            /*
             * Query Partitioning-Where check
             */
            ComparatorHelper.assumeResultSetsAreEqual(qResultSet, qpResultSet, qString, combinedString, state,
                    DataFusionUtil::cleanResultSetString);
        } catch (AssertionError e) {
            // Append more error message
            String replay = DataFusionUtil.getReplay(state.getDatabaseName());
            String newMessage = e.getMessage() + "\n" + e.getCause() + "\n" + replay + "\n" + "Query Result: "
                    + qResultSet + "\nPartitioned Query Result: " + qpResultSet + "\n";
            state.dfLogger.appendToLog(ERROR, newMessage);

            throw new AssertionError(newMessage);
        }
    }
}
