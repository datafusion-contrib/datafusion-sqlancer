package sqlancer.datafusion.gen;

import static sqlancer.datafusion.DataFusionUtil.dfAssert;
import static sqlancer.datafusion.gen.DataFusionBaseExprFactory.createExpr;
import static sqlancer.datafusion.gen.DataFusionBaseExprFactory.getExprsWithReturnType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.NewFunctionNode;
import sqlancer.common.ast.newast.NewOrderingTerm;
import sqlancer.common.ast.newast.NewUnaryPostfixOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPrefixOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.gen.TypedExpressionGenerator;
import sqlancer.datafusion.DataFusionProvider.DataFusionGlobalState;
import sqlancer.datafusion.DataFusionSchema.DataFusionColumn;
import sqlancer.datafusion.DataFusionSchema.DataFusionDataType;
import sqlancer.datafusion.ast.DataFusionExpression;
import sqlancer.datafusion.gen.DataFusionBaseExpr.ArgumentType;
import sqlancer.datafusion.gen.DataFusionBaseExpr.DataFusionBaseExprCategory;
import sqlancer.datafusion.gen.DataFusionBaseExpr.DataFusionBaseExprType;

public final class DataFusionExpressionGenerator
        extends TypedExpressionGenerator<Node<DataFusionExpression>, DataFusionColumn, DataFusionDataType> {

    private final DataFusionGlobalState globalState;
    public boolean supportAggregate; // control if generate aggr exprs, related logic is in `generateExpression()`

    public DataFusionExpressionGenerator(DataFusionGlobalState globalState) {
        this.globalState = globalState;
        supportAggregate = false;
    }

    @Override
    protected DataFusionDataType getRandomType() {
        DataFusionDataType dt;
        do {
            dt = Randomly.fromOptions(DataFusionDataType.values());
        } while (dt == DataFusionDataType.NULL);

        return dt;
    }

    @Override
    protected boolean canGenerateColumnOfType(DataFusionDataType type) {
        return true;
    }

    // If target expr type is numeric, when `supportAggregate`, make it more likely to generate aggregate functions
    private boolean filterBaseExpr(DataFusionBaseExpr expr, DataFusionDataType type) {
        // keep only aggregates
        if (supportAggregate && type.isNumeric() && Randomly.getBoolean()) {
            return expr.exprType == DataFusionBaseExpr.DataFusionBaseExprCategory.AGGREGATE;
        }

        // keep all avaialble expressions (aggr + non-aggr)
        if (supportAggregate || Randomly.getBooleanWithRatherLowProbability()) {
            return true;
        }

        // keep all non-aggregate exprs
        return expr.exprType != DataFusionBaseExprCategory.AGGREGATE;
    }

    // By default all possible non-aggregate expressions
    // To generate aggregate functions: set this.supportAggregate to `true`, generate exprs, and reset.
    @Override
    protected Node<DataFusionExpression> generateExpression(DataFusionDataType type, int depth) {
        if (depth >= globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            DataFusionDataType expectedType = type;
            if (Randomly.getBooleanWithRatherLowProbability()) { // ~10%
                expectedType = DataFusionDataType.getRandomWithoutNull();
            }
            return generateLeafNode(expectedType);
        }

        List<DataFusionBaseExpr> possibleBaseExprs = getExprsWithReturnType(Optional.of(type)).stream()
                .filter(expr -> filterBaseExpr(expr, type)).collect(Collectors.toList());

        if (possibleBaseExprs.isEmpty()) {
            dfAssert(type == DataFusionDataType.NULL, "should able to generate expression with type " + type);
            return generateLeafNode(type);
        }

        DataFusionBaseExpr randomExpr = Randomly.fromList(possibleBaseExprs);
        switch (randomExpr.exprType) {
        case UNARY_PREFIX:
            DataFusionDataType argType = null;
            dfAssert(randomExpr.argTypes.size() == 1 && randomExpr.nArgs == 1,
                    "Unary expression should only have 1 argument" + randomExpr.argTypes);
            if (randomExpr.argTypes.get(0) instanceof ArgumentType.Fixed) {
                ArgumentType.Fixed possibleArgTypes = (ArgumentType.Fixed) randomExpr.argTypes.get(0);
                argType = Randomly.fromList(possibleArgTypes.fixedType);
            } else {
                argType = type;
            }

            return new NewUnaryPrefixOperatorNode<DataFusionExpression>(generateExpression(argType, depth + 1),
                    randomExpr);
        case UNARY_POSTFIX:
            dfAssert(randomExpr.argTypes.size() == 1 && randomExpr.nArgs == 1,
                    "Unary expression should only have 1 argument" + randomExpr.argTypes);
            if (randomExpr.argTypes.get(0) instanceof ArgumentType.Fixed) {
                ArgumentType.Fixed possibleArgTypes = (ArgumentType.Fixed) randomExpr.argTypes.get(0);
                argType = Randomly.fromList(possibleArgTypes.fixedType);
            } else {
                argType = type;
            }

            return new NewUnaryPostfixOperatorNode<DataFusionExpression>(generateExpression(argType, depth + 1),
                    randomExpr);
        case BINARY:
            dfAssert(randomExpr.argTypes.size() == 2 && randomExpr.nArgs == 2,
                    "Binrary expression should only have 2 argument" + randomExpr.argTypes);
            List<DataFusionDataType> argTypeList = new ArrayList<>(); // types of current expression's input arguments
            for (ArgumentType argumentType : randomExpr.argTypes) {
                if (argumentType instanceof ArgumentType.Fixed) {
                    ArgumentType.Fixed possibleArgTypes = (ArgumentType.Fixed) randomExpr.argTypes.get(0);
                    dfAssert(!possibleArgTypes.fixedType.isEmpty(), "possible types can't be an empty list");
                    DataFusionDataType determinedType = Randomly.fromList(possibleArgTypes.fixedType);
                    argTypeList.add(determinedType);
                } else if (argumentType instanceof ArgumentType.SameAsFirstArgType) {
                    dfAssert(!argTypeList.isEmpty(), "First argument can't have argument type `SameAsFirstArgType`");
                    DataFusionDataType firstArgType = argTypeList.get(0);
                    argTypeList.add(firstArgType);
                } else {
                    // Same as expression return type
                    argTypeList.add(type);
                }
            }

            return new NewBinaryOperatorNode<DataFusionExpression>(generateExpression(argTypeList.get(0), depth + 1),
                    generateExpression(argTypeList.get(1), depth + 1), randomExpr);
        case AGGREGATE:
            // Fall through
        case FUNC:
            return generateFunctionExpression(type, depth, randomExpr);
        default:
            dfAssert(false, "unreachable");
        }

        dfAssert(false, "unreachable");
        return null;
    }

    public Node<DataFusionExpression> generateFunctionExpression(DataFusionDataType type, int depth,
            DataFusionBaseExpr exprType) {
        if (Randomly.getBooleanWithSmallProbability()) {
            int nArgs = (int) Randomly.getNotCachedInteger(0, 5);
            return new NewFunctionNode<DataFusionExpression, DataFusionBaseExpr>(generateExpressions(nArgs), exprType);
        }

        if (exprType.isVariadic) {
            int nArgs = (int) Randomly.getNotCachedInteger(0, 5);
            dfAssert(exprType.argTypes.get(0) instanceof ArgumentType.Fixed,
                    "variadic function must specify possible argument types");
            List<DataFusionDataType> possibleTypes = ((ArgumentType.Fixed) exprType.argTypes.get(0)).fixedType;

            List<Node<DataFusionExpression>> argExprs = new ArrayList<>();
            for (int i = 0; i < nArgs; i++) {
                argExprs.add(generateExpression(Randomly.fromList(possibleTypes)));
            }

            return new NewFunctionNode<DataFusionExpression, DataFusionBaseExpr>(argExprs, exprType);
        }

        List<DataFusionDataType> funcArgTypeList = new ArrayList<>(); // types of current expression's input arguments
        int i = 0;
        for (ArgumentType argumentType : exprType.argTypes) {
            if (argumentType instanceof ArgumentType.Fixed) {
                ArgumentType.Fixed possibleArgTypes = (ArgumentType.Fixed) exprType.argTypes.get(i);
                dfAssert(!possibleArgTypes.fixedType.isEmpty(), "possible types can't be an empty list");
                DataFusionDataType determinedType = Randomly.fromList(possibleArgTypes.fixedType);
                funcArgTypeList.add(determinedType);
            } else if (argumentType instanceof ArgumentType.SameAsFirstArgType) {
                dfAssert(!funcArgTypeList.isEmpty(), "First argument can't have argument type `SameAsFirstArgType`");
                DataFusionDataType firstArgType = funcArgTypeList.get(0);
                funcArgTypeList.add(firstArgType);
            } else {
                // Same as expression return type
                funcArgTypeList.add(type);
            }
            i++;
        }

        List<Node<DataFusionExpression>> argExpressions = new ArrayList<>();

        for (DataFusionDataType dataType : funcArgTypeList) {
            argExpressions.add(generateExpression(dataType, depth + 1));
        }

        return new NewFunctionNode<DataFusionExpression, DataFusionBaseExpr>(argExpressions, exprType);
    }

    List<DataFusionColumn> filterColumns(DataFusionDataType type) {
        if (columns == null) {
            return Collections.emptyList();
        } else {
            return columns.stream().filter(c -> c.getType() == type).collect(Collectors.toList());
        }
    }

    // Duplicate column is possible (e.g. v1, v2, v1)
    public List<Node<DataFusionExpression>> generateColumns(int nr) {
        List<Node<DataFusionExpression>> cols = new ArrayList<>();
        for (int i = 0; i < nr; i++) {
            if (columns.isEmpty()) {
                cols.add(generateColumn(getRandomType()));
            } else {
                DataFusionColumn col = Randomly.fromList(columns);
                Node<DataFusionExpression> colExpr = new ColumnReferenceNode<>(col);
                cols.add(colExpr);
            }
        }

        return cols;
    }

    @Override
    public List<Node<DataFusionExpression>> generateOrderBys() {
        List<Node<DataFusionExpression>> expr = super.generateOrderBys();
        List<Node<DataFusionExpression>> newExpr = new ArrayList<>(expr.size());

        for (Node<DataFusionExpression> curExpr : expr) {
            if (Randomly.getBoolean()) {
                if (Randomly.getBoolean()) {
                    // e.g. [curExpr] ASC
                    curExpr = new NewOrderingTerm<>(curExpr, NewOrderingTerm.Ordering.getRandom());
                } else {
                    // e.g. [curExpr] ASC NULLS LAST
                    curExpr = new NewOrderingTerm<>(curExpr, NewOrderingTerm.Ordering.getRandom(),
                            NewOrderingTerm.OrderingNulls.getRandom());
                }

            }
            newExpr.add(curExpr);
        }
        return newExpr;
    }

    @Override
    protected Node<DataFusionExpression> generateColumn(DataFusionDataType type) {
        // HACK: if no col of such type exist, generate constant value instead
        List<DataFusionColumn> colsOfType = filterColumns(type);
        if (colsOfType.isEmpty()) {
            return generateConstant(type);
        }

        DataFusionColumn column = Randomly.fromList(colsOfType);
        return new ColumnReferenceNode<DataFusionExpression, DataFusionColumn>(column);
    }

    @Override
    public Node<DataFusionExpression> generateConstant(DataFusionDataType type) {
        return type.getRandomConstant(globalState);
    }

    @Override
    public Node<DataFusionExpression> generatePredicate() {
        return generateExpression(DataFusionDataType.BOOLEAN, 0);
    }

    @Override
    public Node<DataFusionExpression> negatePredicate(Node<DataFusionExpression> predicate) {
        return new NewUnaryPrefixOperatorNode<>(predicate, createExpr(DataFusionBaseExprType.NOT));
    }

    @Override
    public Node<DataFusionExpression> isNull(Node<DataFusionExpression> expr) {
        return new NewUnaryPostfixOperatorNode<>(expr, createExpr(DataFusionBaseExprType.IS_NULL));
    }

    // TODO(datafusion) refactor: make single generate aware of group by and aggr columns, and it can directly generate
    // having clause
    // Try best to generate a valid having clause
    //
    // Suppose query "... group by a, b ..."
    // and all available columns are "a, b, c, d"
    // then a valid having clause can have expr of {a, b}, and expr of aggregation of {c, d}
    // e.g. "having a=b and avg(c) > avg(d)"
    //
    // `groupbyGen` can generate expression only with group by cols
    // `aggrGen` can generate expression only with aggr cols
    public static Node<DataFusionExpression> generateHavingClause(DataFusionExpressionGenerator groupbyGen,
            DataFusionExpressionGenerator aggrGen) {
        if (Randomly.getBoolean()) {
            return groupbyGen.generatePredicate();
        } else {
            aggrGen.supportAggregate = true;
            Node<DataFusionExpression> expr = aggrGen.generatePredicate();
            aggrGen.supportAggregate = false;
            return expr;
        }
    }

    public static class DataFusionCastOperation extends NewUnaryPostfixOperatorNode<DataFusionExpression> {

        public DataFusionCastOperation(Node<DataFusionExpression> expr, DataFusionDataType type) {
            super(expr, new Operator() {

                @Override
                public String getTextRepresentation() {
                    return "::" + type.toString();
                }
            });
        }

    }

}
