package sqlancer.datafusion.test;

import static sqlancer.datafusion.DataFusionUtil.DataFusionLogger.DataFusionLogType.ERROR;
import static sqlancer.datafusion.ast.DataFusionSelect.getRandomSelect;
import static sqlancer.datafusion.ast.DataFusionWindowExpr.getRandomWindowClause;

import java.sql.SQLException;
import java.util.Arrays;

import sqlancer.ComparatorHelper;
import sqlancer.common.oracle.NoRECBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.datafusion.DataFusionErrors;
import sqlancer.datafusion.DataFusionProvider.DataFusionGlobalState;
import sqlancer.datafusion.DataFusionToStringVisitor;
import sqlancer.datafusion.DataFusionUtil;
import sqlancer.datafusion.ast.DataFusionSelect;
import sqlancer.datafusion.ast.DataFusionWindowExpr;

// Simply test no crash bug for generated queries.
// No extra oracle checks.
public class DataFusionNoCrashWindow extends NoRECBase<DataFusionGlobalState>
        implements TestOracle<DataFusionGlobalState> {

    private final DataFusionGlobalState state;

    public DataFusionNoCrashWindow(DataFusionGlobalState globalState) {
        super(globalState);
        this.state = globalState;
        DataFusionErrors.registerExpectedExecutionErrors(errors);
    }

    // Randomly generate an aggregate query.
    // And make sure it won't crash DataFusion engine
    @Override
    public void check() throws SQLException {
        DataFusionSelect randomSelect = getRandomSelect(state);
        DataFusionWindowExpr windowExpr = getRandomWindowClause(randomSelect.exprGenAll, this.state);
        randomSelect.setFetchColumns(Arrays.asList(windowExpr));

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
