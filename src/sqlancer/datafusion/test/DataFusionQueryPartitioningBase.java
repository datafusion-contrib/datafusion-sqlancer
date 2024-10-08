package sqlancer.datafusion.test;

import java.sql.SQLException;

import sqlancer.common.ast.newast.Node;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.datafusion.DataFusionProvider.DataFusionGlobalState;
import sqlancer.datafusion.ast.DataFusionExpression;
import sqlancer.datafusion.ast.DataFusionSelect;
import sqlancer.datafusion.gen.DataFusionExpressionGenerator;

public class DataFusionQueryPartitioningBase
        extends TernaryLogicPartitioningOracleBase<Node<DataFusionExpression>, DataFusionGlobalState>
        implements TestOracle<DataFusionGlobalState> {
    DataFusionGlobalState state;
    // Generate expression given available columns
    // This includes all columns to generate WHERE
    // see DataFusionSelect's comment for other expression generators
    DataFusionExpressionGenerator gen;
    DataFusionSelect select;

    public DataFusionQueryPartitioningBase(DataFusionGlobalState state) {
        super(state);
        this.state = state;
    }

    @Override
    public void check() throws SQLException {
        select = DataFusionSelect.getRandomSelect(state);
        gen = select.exprGenAll;
        initializeTernaryPredicateVariants();
    }

    @Override
    protected ExpressionGenerator<Node<DataFusionExpression>> getGen() {
        return gen;
    }

}
