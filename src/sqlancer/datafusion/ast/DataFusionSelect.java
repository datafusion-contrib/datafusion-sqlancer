package sqlancer.datafusion.ast;

import static sqlancer.datafusion.DataFusionUtil.dfAssert;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.ast.SelectBase;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.datafusion.DataFusionProvider.DataFusionGlobalState;
import sqlancer.datafusion.DataFusionSchema;
import sqlancer.datafusion.DataFusionSchema.DataFusionColumn;
import sqlancer.datafusion.DataFusionSchema.DataFusionTable;
import sqlancer.datafusion.gen.DataFusionExpressionGenerator;

public class DataFusionSelect extends SelectBase<Node<DataFusionExpression>> implements Node<DataFusionExpression> {
    public boolean all = false; // SELECT ALL
    public boolean distinct = false; // SELECT DISTINCT
    public Optional<String> fetchColumnsString = Optional.empty(); // When available, override `fetchColumns` in base
    // class's `Node` representation (for display)

    // `from` is used to represent from table list and join clause
    // `fromList` and `joinList` in base class should always be empty
    public DataFusionFrom from;
    // e.g. let's say all colummns are {c1, c2, c3, c4, c5}
    // First randomly pick a subset say {c2, c1, c3, c4}
    // `exprGenAll` can generate random expr using above 4 columns
    //
    // Next, randomly take two non-overlapping subset from all columns used by `exprGenAll`
    // exprGenGroupBy: {c1} (randomly generate group by exprs using c1 only)
    // exprGenAggregate: {c3, c4}
    //
    // Finally, use all `Gen`s to generate different clauses in a query (`exprGenAll` in where clause, `exprGenGroupBy`
    // in group by clause, etc.)
    public DataFusionExpressionGenerator exprGenAll;
    public DataFusionExpressionGenerator exprGenGroupBy;
    public DataFusionExpressionGenerator exprGenAggregate;

    public enum JoinType {
        INNER, LEFT, RIGHT, FULL, CROSS, NATURAL
    }

    // DataFusionFrom can be used to represent from table list or join list
    // 1. When `joinConditionList` is empty, then it's a table list (implicit cross join)
    // join condition can be generated in `WHERE` clause (outside `FromClause`)
    // e.g. select * from [expr], [expr] is t1, t3, t2
    // - tableList -> {t1, t3,t2}
    // - predicateList -> null
    // 2. When `joinConditionList` is not empty, the from-clause is a join list
    // e.g.
    // select * from t1
    // join t2 on t1.v1=t2.v1
    // left join t3 on t1.v1=t2.v1 and t1.v2=t3.v2
    // - tableList -> {t1, t2, t3}
    // - joinTypeList -> {INNER, LEFT}
    // - joinConditionList -> {[expr_with_t1_t2], [expr_with_t1_t2_t3]}
    public static class DataFusionFrom implements Node<DataFusionExpression> {
        public List<Node<DataFusionExpression>> tableList;
        public List<JoinType> joinTypeList;
        public List<Node<DataFusionExpression>> joinConditionList;

        public DataFusionFrom() {
            tableList = new ArrayList<>();
            joinTypeList = new ArrayList<>();
            joinConditionList = new ArrayList<>();
        }

        public boolean isExplicitJoin() {
            // if it's explicit join, joinTypeList and joinConditionList should be both length of tableList.len - 1
            // Otherwise, both is empty
            dfAssert(joinTypeList.size() == joinConditionList.size(), "Validate FromClause");
            return !joinTypeList.isEmpty();
        }

        // Randomly generate a FromClause
        // TODO(datafusion) support self join 'select * from t1, t1 as t1a'
        // TODO(datafusion) support using 'select * from t1 join t2 using(v0)'
        public static DataFusionFrom generateFromClause(DataFusionGlobalState state,
                List<DataFusionTable> randomTables) {
            DataFusionFrom fromClause = new DataFusionFrom(); // return result

            /* Setup tableList */
            dfAssert(!randomTables.isEmpty(), "Must have some tables");
            List<Node<DataFusionExpression>> randomTableNodes = randomTables.stream()
                    .map(t -> new TableReferenceNode<DataFusionExpression, DataFusionTable>(t))
                    .collect(Collectors.toList());
            fromClause.tableList = randomTableNodes;

            /* If JoinConditionList is empty, FromClause will be interpreted as from list */
            if (Randomly.getBoolean() && Randomly.getBoolean()) {
                return fromClause;
            }

            /* Set fromClause's joinTypeList and joinConditionList */
            List<DataFusionColumn> possibleColsToGenExpr = new ArrayList<>();
            possibleColsToGenExpr.addAll(randomTables.get(0).getColumns()); // first table
            // Generate join conditions (see class-level comment example's joinConditionList)
            //
            // Join Type | `ON` Clause Requirement
            // INNER JOIN | Required
            // LEFT OUTER JOIN | Required
            // RIGHT OUTER JOIN | Required
            // FULL OUTER JOIN | Required
            // CROSS JOIN | Not allowed
            // NATURAL JOIN | Not allowed
            // JOIN with USING | Optional
            for (int i = 1; i < randomTables.size(); i++) {
                JoinType randomJoinType = Randomly.fromOptions(JoinType.values());
                fromClause.joinTypeList.add(randomJoinType);
                possibleColsToGenExpr.addAll(randomTables.get(i).getColumns());
                DataFusionExpressionGenerator exprGen = new DataFusionExpressionGenerator(state)
                        .setColumns(possibleColsToGenExpr);
                if (randomJoinType == JoinType.CROSS || randomJoinType == JoinType.NATURAL) {
                    if (Randomly.getBooleanWithSmallProbability()) {
                        fromClause.joinConditionList
                                .add(exprGen.generateExpression(DataFusionSchema.DataFusionDataType.BOOLEAN));
                    } else {
                        fromClause.joinConditionList.add(null);
                    }
                } else {
                    if (Randomly.getBooleanWithSmallProbability()) {
                        fromClause.joinConditionList.add(null);
                    } else {
                        fromClause.joinConditionList
                                .add(exprGen.generateExpression(DataFusionSchema.DataFusionDataType.BOOLEAN));
                    }
                }
                // TODO(datafusion) make join conditions more likely to be 'col1=col2', also some join types don't have
                // 'ON' condition
            }

            return fromClause;
        }
    }

    // Generate SELECT statement according to the dependency of exprs, e.g.:
    // SELECT [expr_groupby_cols], [expr_aggr_cols]
    // FROM [from_clause]
    // WHERE [expr_all_cols]
    // GROUP BY [expr_groupby_cols]
    // HAVING [expr_gorupby_cols], [expr_aggr_cols]
    // ORDER BY [expr_gorupby_cols], [expr_aggr_cols]
    // LIMIT [constant]
    //
    // The generation order will be:
    // 1. [from_clause] - Pick tables like t1, t2, t3 and get a join clause
    // 2. [expr_all_cols] - Generate a non-aggregate expression with all columns in t1, t2, t3. e.g.:
    // - t1.v1 = t2.v1 and t1.v2 > t3.v2
    // 3. [expr_groupby_cols], [expr_aggr_cols] - Randomly pick some cols in t1, t2, t3 as group by columns, and pick
    // some other columns as aggregation columns, and generate non-aggr expression [expr_groupby_cols] on group by
    // columns, finally generate aggregation expressions [expr_aggr_cols] on non-group-by/aggregation columns.
    // For example, group by column is t1.v1, and aggregate columns is t2.v1, t3.v1, generated expressions can be:
    // - [expr_groupby_cols] t1.v1 + 1
    // - [expr_aggr_cols] SUM(t3.v1 + t2.v1)
    public static DataFusionSelect getRandomSelect(DataFusionGlobalState state) {
        DataFusionSelect randomSelect = new DataFusionSelect();
        if (Randomly.getBooleanWithRatherLowProbability()) {
            randomSelect.all = true;
        }

        /* Setup FROM clause */
        DataFusionSchema schema = state.getSchema(); // schema of all tables
        List<DataFusionTable> allTables = schema.getDatabaseTables();
        List<DataFusionTable> randomTables = Randomly.nonEmptySubset(allTables);
        int maxSize = Randomly.fromOptions(1, 2, 3);
        if (randomTables.size() > maxSize) {
            randomTables = randomTables.subList(0, maxSize);
        }
        DataFusionFrom randomFrom = DataFusionFrom.generateFromClause(state, randomTables);

        /* Setup expression generators (to generate different clauses) */
        List<DataFusionColumn> randomColumnsAll = DataFusionTable.getRandomColumns(randomTables);
        // 0 <= splitPoint1 <= splitPoint2 < randomColumnsALl.size()
        int splitPoint1 = state.getRandomly().getInteger(0, randomColumnsAll.size());
        int splitPoint2 = state.getRandomly().getInteger(splitPoint1, randomColumnsAll.size());

        randomSelect.exprGenAll = new DataFusionExpressionGenerator(state).setColumns(randomColumnsAll);
        randomSelect.exprGenGroupBy = new DataFusionExpressionGenerator(state)
                .setColumns(randomColumnsAll.subList(0, splitPoint1));
        randomSelect.exprGenAggregate = new DataFusionExpressionGenerator(state)
                .setColumns(randomColumnsAll.subList(splitPoint1, splitPoint2));

        /* Setup WHERE clause */
        Node<DataFusionExpression> whereExpr = randomSelect.exprGenAll
                .generateExpression(DataFusionSchema.DataFusionDataType.BOOLEAN);

        /* Constructing result */
        List<Node<DataFusionExpression>> randomColumnNodes = randomColumnsAll.stream()
                .map((c) -> new ColumnReferenceNode<DataFusionExpression, DataFusionColumn>(c))
                .collect(Collectors.toList());

        randomSelect.setFetchColumns(randomColumnNodes); // TODO(datafusion) make it more random like 'select *'
        randomSelect.from = randomFrom;
        randomSelect.setWhereClause(whereExpr);
        // // if explicit join (from t1 join t2), 50% case generate where clause
        // // if join is implicit (from t1, t2), 90% case generate where
        // if (randomFrom.isExplicitJoin()) {
        // if (Randomly.getBoolean()) {
        // randomSelect.setWhereClause(whereExpr);
        // }
        // } else {
        // if (!Randomly.getBooleanWithRatherLowProbability()) {
        // randomSelect.setWhereClause(whereExpr);
        // }
        // }

        return randomSelect;
    }

    /*
     * If set fetch columns with string It will override `fetchColumns` in base class when
     * `DataFusionToStringVisitor.asString()` is called
     *
     * This method can be helpful to mutate select in oracle checks: SELECT [expr] ... -> SELECT SUM[expr]
     */
    public void setFetchColumnsString(String selectExpr) {
        this.fetchColumnsString = Optional.of(selectExpr);
    }
}
