package sqlancer.datafusion.ast;

import static sqlancer.datafusion.gen.DataFusionExpressionGenerator.getIntegerPreferSmallPositive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.datafusion.DataFusionProvider;
import sqlancer.datafusion.DataFusionSchema;
import sqlancer.datafusion.gen.DataFusionExpressionGenerator;

// This Node is for single window expression
// e.g. `rank() over()`
public class DataFusionWindowExpr implements Node<DataFusionExpression> {
    // Window expression syntax reference:
    // function([expr])
    // OVER(
    // [PARTITION BY expr[, …]]
    // [ORDER BY expr [ ASC | DESC ][, …]]
    // [ frame_clause ]
    // )

    // Window Function
    // ===============
    public Node<DataFusionExpression> windowFunc;

    // Over Clause components
    // ======================

    // Optional. Empty list means not present.
    public List<Node<DataFusionExpression>> partitionByList = new ArrayList<>();
    // Optional. Empty list means not present.
    public List<Node<DataFusionExpression>> orderByList = new ArrayList<>();
    // Optional. Empty option means not present.
    public Optional<String> frameClause;

    // Others
    // =======

    // To generate `partitionByList` and `orderByList`
    public DataFusionExpressionGenerator exprGen;

    public static DataFusionWindowExpr getRandomWindowClause(DataFusionExpressionGenerator gen,
            DataFusionProvider.DataFusionGlobalState state) {
        DataFusionWindowExpr windowExpr = new DataFusionWindowExpr();
        windowExpr.exprGen = gen;

        // setup window function e.g. 'rank()'
        windowExpr.exprGen.supportWindow = true;
        windowExpr.windowFunc = windowExpr.exprGen
                .generateExpression(DataFusionSchema.DataFusionDataType.getRandomWithoutNull());
        windowExpr.exprGen.supportWindow = false;

        // setup `partition by`
        windowExpr.partitionByList = windowExpr.exprGen.generateExpressionsPreferColumns();

        // setup `order by`
        windowExpr.orderByList = windowExpr.exprGen.generateOrderBys();

        // setup frame range
        windowExpr.frameClause = DataFusionWindowExprFrame.getRandomFrame(state);

        return windowExpr;
    }

}

// 'frame_clause' is one of:
// { RANGE | ROWS | GROUPS } frame_start
// { RANGE | ROWS | GROUPS } BETWEEN frame_start AND frame_end
//
// 'frame_start' and 'frame_end' can be:
// UNBOUNDED PRECEDING
// offset PRECEDING
// CURRENT ROW
// offset FOLLOWING
// UNBOUNDED FOLLOWING
//
// offset is non-negative integer
// (but this class might generate something else to make it more chaotic)
//
// Reference:
// https://datafusion.apache.org/user-guide/sql/window_functions.html#syntax
final class DataFusionWindowExprFrame {
    private static final List<String> FRAME_TYPES = Arrays.asList("RANGE", "ROWS", "GROUPS");

    // Private constructor to prevent instantiation
    private DataFusionWindowExprFrame() {
    }

    // The range epxression inside now only support integer liternal (not expr)
    // So make it string instead of Node<DataFusionExpression> for simplicity
    public static Optional<String> getRandomFrame(DataFusionProvider.DataFusionGlobalState state) {
        if (Randomly.getBoolean()) {
            return Optional.empty();
        }

        String frameType = Randomly.fromList(FRAME_TYPES);
        String frameStart = generateFramePoint(state, true);
        String frameEnd = generateFramePoint(state, false);

        String repr;
        if (Randomly.getBoolean()) {
            repr = frameType + " " + frameStart;
        } else {
            repr = frameType + " BETWEEN " + frameStart + " AND " + frameEnd;
        }

        return Optional.of(repr);
    }

    private static String generateFramePoint(DataFusionProvider.DataFusionGlobalState state, boolean isStart) {
        int offset = getIntegerPreferSmallPositive(state);
        List<String> options = new ArrayList<>(Arrays.asList("UNBOUNDED PRECEDING", offset + " PRECEDING",
                "CURRENT ROW", offset + " FOLLOWING", "UNBOUNDED FOLLOWING"));

        if (!isStart && !Randomly.getBooleanWithRatherLowProbability()) {
            options.remove("UNBOUNDED PRECEDING");
        }
        return Randomly.fromList(options);
    }
}
