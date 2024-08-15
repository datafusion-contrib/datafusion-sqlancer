package sqlancer.datafusion.test;

import static sqlancer.datafusion.DataFusionUtil.DataFusionLogger.DataFusionLogType.ERROR;
import static sqlancer.datafusion.ast.DataFusionSelect.getRandomSelect;
import static sqlancer.datafusion.gen.DataFusionExpressionGenerator.generateHavingClause;

import java.sql.SQLException;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.oracle.NoRECBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.datafusion.DataFusionErrors;
import sqlancer.datafusion.DataFusionProvider.DataFusionGlobalState;
import sqlancer.datafusion.DataFusionToStringVisitor;
import sqlancer.datafusion.DataFusionUtil;
import sqlancer.datafusion.ast.DataFusionExpression;
import sqlancer.datafusion.ast.DataFusionSelect;
import sqlancer.datafusion.gen.DataFusionExpressionGenerator;

// Simply test no crash bug for generated queries.
// No extra oracle checks.
public class DataFusionNoCrashAggregate extends NoRECBase<DataFusionGlobalState>
        implements TestOracle<DataFusionGlobalState> {

    private final DataFusionGlobalState state;

    public DataFusionNoCrashAggregate(DataFusionGlobalState globalState) {
        super(globalState);
        this.state = globalState;
        DataFusionErrors.registerExpectedExecutionErrors(errors);
    }

    // Randomly generate a aggregate query.
    // And make sure it won't crash DataFusion engine
    @Override
    public void check() throws SQLException {
        DataFusionSelect randomSelect = getRandomSelect(state);
        DataFusionExpressionGenerator gen = randomSelect.exprGenAll;

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

        if (Randomly.getBoolean()) {
            Node<DataFusionExpression> havingPredicate = generateHavingClause(randomSelect.exprGenGroupBy,
                    randomSelect.exprGenAggregate);
            randomSelect.setHavingClause(havingPredicate);
        }

        String qString = DataFusionToStringVisitor.asString(randomSelect);
        try {
            ComparatorHelper.getResultSetFirstColumnAsString(qString, errors, state);
        } catch (AssertionError e) {
            // Append detailed error message
            String replay = DataFusionUtil.getReplay(state.getDatabaseName());
            String newMessage = e.getMessage() + "\n" + e.getCause() + "\n" + replay + "\n";
            state.dfLogger.appendToLog(ERROR, newMessage);

            throw new AssertionError(newMessage);
        }
    }
}
