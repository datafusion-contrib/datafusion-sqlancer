package sqlancer.datafusion.test;

import static sqlancer.datafusion.DataFusionUtil.DataFusionLogger.DataFusionLogType.ERROR;
import static sqlancer.datafusion.gen.DataFusionBaseExprFactory.createExpr;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.datafusion.DataFusionErrors;
import sqlancer.datafusion.DataFusionProvider.DataFusionGlobalState;
import sqlancer.datafusion.DataFusionToStringVisitor;
import sqlancer.datafusion.DataFusionUtil;
import sqlancer.datafusion.ast.DataFusionExpression;
import sqlancer.datafusion.ast.DataFusionSelect;
import sqlancer.datafusion.gen.DataFusionBaseExpr.DataFusionBaseExprType;

public class DataFusionQueryPartitioningWhereTester extends DataFusionQueryPartitioningBase {
    public DataFusionQueryPartitioningWhereTester(DataFusionGlobalState state) {
        super(state);
        DataFusionErrors.registerExpectedExecutionErrors(errors);
    }

    /*
     * Query Partitioning - Where
     *
     * q: SELECT [expr1] FROM [expr2]
     *
     * qp1: SELECT [expr1] FROM [expr2] WHERE [expr3]
     *
     * qp2: SELECT [expr1] FROM [expr2] WHERE NOT [expr3]
     *
     * qp3: SELECT [expr1] FROM [expr2] WHERE [expr3] IS NULL
     *
     * Oracle check: q's result equals to union(qp1, qp2, qp3)
     */
    @Override
    public void check() throws SQLException {
        // generate a random 'SELECT [expr1] FROM [expr2] WHERE [expr3]
        super.check();
        DataFusionSelect randomSelect = select;

        if (Randomly.getBoolean()) {
            randomSelect.distinct = true;
        }

        if (Randomly.getBoolean()) {
            randomSelect.setOrderByClauses(gen.generateOrderBys());
        }

        if (Randomly.getBoolean() && Randomly.getBoolean()) {
            randomSelect.setGroupByClause(
                    randomSelect.exprGenGroupBy.generateExpressions(state.getRandomly().getInteger(1, 3)));

            // if (Randomly.getBoolean()) {
            // randomSelect.setHavingClause(randomSelect.exprGenGroupBy.generatePredicate());
            // }
        }

        String qString = "";
        String qp1String = "";
        String qp2String = "";
        String qp3String = "";
        if (Randomly.getBoolean()) {
            randomSelect.setWhereClause(null);
            qString = DataFusionToStringVisitor.asString(randomSelect);

            randomSelect.setWhereClause(predicate);
            qp1String = DataFusionToStringVisitor.asString(randomSelect);

            randomSelect.setWhereClause(negatedPredicate);
            qp2String = DataFusionToStringVisitor.asString(randomSelect);

            randomSelect.setWhereClause(isNullPredicate);
            qp3String = DataFusionToStringVisitor.asString(randomSelect);
        } else {
            // Extended TLP-WHERE
            //
            // select * from t1 where pExist
            // ---------------------------------------------
            // select * from t1 where pExist AND p
            // select * from t1 where pExist AND (NOT p)
            // select * from t1 where pExist AND (p IS NULL)
            Node<DataFusionExpression> pExist = gen.generatePredicate();
            Node<DataFusionExpression> p1 = new NewBinaryOperatorNode<>(pExist, predicate,
                    createExpr(DataFusionBaseExprType.AND));
            Node<DataFusionExpression> p2 = new NewBinaryOperatorNode<>(pExist, negatedPredicate,
                    createExpr(DataFusionBaseExprType.AND));
            Node<DataFusionExpression> p3 = new NewBinaryOperatorNode<>(pExist, isNullPredicate,
                    createExpr(DataFusionBaseExprType.AND));

            randomSelect.setWhereClause(pExist);

            qString = DataFusionToStringVisitor.asString(randomSelect);

            randomSelect.setWhereClause(p1);
            qp1String = DataFusionToStringVisitor.asString(randomSelect);

            randomSelect.setWhereClause(p2);
            qp2String = DataFusionToStringVisitor.asString(randomSelect);

            randomSelect.setWhereClause(p3);
            qp3String = DataFusionToStringVisitor.asString(randomSelect);
        }

        try {
            /*
             * Run all queires
             */
            List<String> qResultSet = ComparatorHelper.getResultSetFirstColumnAsString(qString, errors, state);
            List<String> combinedString = new ArrayList<>();
            List<String> qpResultSet = ComparatorHelper.getCombinedResultSet(qp1String, qp2String, qp3String,
                    combinedString, true, state, errors);
            /*
             * Query Partitioning-Where check
             */
            ComparatorHelper.assumeResultSetsAreEqual(qResultSet, qpResultSet, qString, combinedString, state,
                    ComparatorHelper::canonicalizeResultValue);
        } catch (AssertionError e) {
            // Append more error message
            String replay = DataFusionUtil.getReplay(state.getDatabaseName());
            String newMessage = e.getMessage() + "\n" + e.getCause() + "\n" + replay + "\n";
            state.dfLogger.appendToLog(ERROR, newMessage);

            throw new AssertionError(newMessage);
        }
    }
}
