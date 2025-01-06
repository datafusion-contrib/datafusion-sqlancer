package sqlancer.datafusion.test;

import static sqlancer.datafusion.DataFusionUtil.dfAssert;
import static sqlancer.datafusion.ast.DataFusionSelect.getRandomSelect;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import sqlancer.IgnoreMeException;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.oracle.NoRECBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.datafusion.DataFusionErrors;
import sqlancer.datafusion.DataFusionProvider.DataFusionGlobalState;
import sqlancer.datafusion.DataFusionSchema.DataFusionColumn;
import sqlancer.datafusion.DataFusionSchema.DataFusionTable;
import sqlancer.datafusion.DataFusionToStringVisitor;
import sqlancer.datafusion.DataFusionUtil;
import sqlancer.datafusion.DataFusionUtil.DataFusionLogger;
import sqlancer.datafusion.ast.DataFusionExpression;
import sqlancer.datafusion.ast.DataFusionSelect;
import sqlancer.datafusion.ast.DataFusionSelect.DataFusionFrom;

public class DataFusionPQS extends NoRECBase<DataFusionGlobalState> implements TestOracle<DataFusionGlobalState> {

    private final DataFusionGlobalState state;

    // Table references in a randomly generated SELECT query
    // used for current PQS check.
    // To construct queries ONLY used in PQS
    // columns will be temporarily aliased
    // Rember to reset them when PQS check is done
    private List<DataFusionTable> pqsTables;

    private StringBuilder currentCheckLog; // Each append should end with '\n'

    public DataFusionPQS(DataFusionGlobalState globalState) {
        super(globalState);
        this.state = globalState;

        DataFusionErrors.registerExpectedExecutionErrors(errors);
    }

    private void setColumnAlias(List<DataFusionTable> tables) {
        List<DataFusionColumn> allColumns = tables.stream().flatMap(t -> t.getColumns().stream())
                .collect(Collectors.toList());
        for (int i = 0; i < allColumns.size(); i++) {
            String alias = "cc" + i;
            allColumns.get(i).alias = Optional.of(alias);
        }
    }

    private void resetColumnAlias(List<DataFusionTable> tables) {
        tables.stream().flatMap(t -> t.getColumns().stream()).forEach(c -> c.alias = Optional.empty());
    }

    private void pqsCleanUp() throws SQLException {
        if (pqsTables == null) {
            return;
        }
        resetColumnAlias(pqsTables);

        // Drop temp tables used in PQS check
        SQLQueryAdapter tttCleanUp = new SQLQueryAdapter("drop table if exists ttt", errors);
        tttCleanUp.execute(state);
        SQLQueryAdapter ttCleanUp = new SQLQueryAdapter("drop table if exists tt", errors);
        ttCleanUp.execute(state);
    }

    @Override
    public void check() throws SQLException {
        this.currentCheckLog = new StringBuilder();
        String replay = DataFusionUtil.getReplay(state.getDatabaseName());
        currentCheckLog.append(replay);
        pqsCleanUp();

        try {
            checkImpl();
        } catch (Exception | AssertionError e) {
            pqsCleanUp();
            throw e;
        }
        pqsCleanUp();
    }

    public void checkImpl() throws SQLException {
        // ======================================================
        // Step 1:
        // select tt0.c0 as cc0, tt0.c1 as cc1, tt1.c0 as cc2
        // from t0 as tt0, t1 as tt1
        // where tt0.c0 = tt1.c0;
        // ======================================================
        DataFusionSelect randomSelect = getRandomSelect(state);
        randomSelect.from.joinConditionList = new ArrayList<>();
        randomSelect.from.joinTypeList = new ArrayList<>();
        randomSelect.mutateEquivalentTableName();
        pqsTables = randomSelect.tableList;

        // Reset fetch columns
        List<DataFusionColumn> allColumns = randomSelect.tableList.stream().flatMap(t -> t.getColumns().stream())
                .collect(Collectors.toList());
        setColumnAlias(pqsTables);
        List<String> aliasedColumns = allColumns.stream().map(c -> c.getOrignalName() + " AS " + c.alias.get())
                .collect(Collectors.toList());
        String fetchColumnsString = String.join(", ", aliasedColumns);
        randomSelect.setFetchColumnsString(fetchColumnsString);

        // ======================================================
        // Step 2:
        // create table tt as
        // with cte0 as (select tt0.c0 as cc0, tt0.c1 as cc1 from t0 as tt0 order by
        // random() limit 1),
        // with cte1 as (select tt1.c0 as cc2 from t1 as tt1 order by random() limit 1)
        // select * from cte0, cte1;
        // ======================================================
        List<DataFusionSelect> cteSelects = new ArrayList<>();
        for (DataFusionTable table : randomSelect.tableList) {
            DataFusionSelect cteSelect = new DataFusionSelect();
            DataFusionFrom cteFrom = new DataFusionFrom(Arrays.asList(table));
            cteSelect.from = cteFrom;

            List<DataFusionColumn> columns = table.getColumns();
            List<String> cteAliasedColumns = columns.stream().map(c -> c.getOrignalName() + " AS " + c.alias.get())
                    .collect(Collectors.toList());
            String cteFetchColumnsString = String.join(", ", cteAliasedColumns);
            cteSelect.setFetchColumnsString(cteFetchColumnsString);
            cteSelects.add(cteSelect);
        }

        // select tt0.c0 as cc0, tt0.c1 as cc1 from tt0 order by random()
        List<String> cteSelectsPickOneRow = cteSelects.stream()
                .map(cteSelect -> DataFusionToStringVisitor.asString(cteSelect) + " ORDER BY RANDOM() LIMIT 1")
                .collect(Collectors.toList());
        int ncte = cteSelectsPickOneRow.size();

        List<String> ctes = new ArrayList<>();
        for (int i = 0; i < ncte; i++) {
            String cte = "cte" + i + " AS (" + cteSelectsPickOneRow.get(i) + ")";
            ctes.add(cte);
        }

        // cte0, cte1, cte2
        List<String> ctesInFrom = IntStream.range(0, ncte).mapToObj(i -> "cte" + i).collect(Collectors.toList());
        String ctesInFromString = String.join(", ", ctesInFrom);

        // Create tt (Table with one pivot row)
        String ttCreate = "CREATE TABLE tt AS\n  WITH\n    " + String.join(",\n    ", ctes) + "\n" + "SELECT * FROM "
                + ctesInFromString;

        currentCheckLog.append("==== Create tt (Table with one pivot row):\n").append(ttCreate).append("\n");

        // ======================================================
        // Step3:
        // Find the predicate that can select the predicate row
        // can be {p, NOT p, p is NULL}
        // Note 'p' is 'cc0 = cc2' in Step 1
        // ======================================================
        Node<DataFusionExpression> whereExpr = randomSelect.getWhereClause(); // must be valid
        Node<DataFusionExpression> notWhereExpr = randomSelect.exprGenAll.negatePredicate(whereExpr);
        Node<DataFusionExpression> isNullWhereExpr = randomSelect.exprGenAll.isNull(whereExpr);
        List<Node<DataFusionExpression>> candidatePredicates = Arrays.asList(whereExpr, notWhereExpr, isNullWhereExpr);

        String pivotQ1 = "select * from tt where " + DataFusionToStringVisitor.asString(whereExpr);
        String pivotQ2 = "select * from tt where " + DataFusionToStringVisitor.asString(notWhereExpr);
        String pivotQ3 = "select * from tt where " + DataFusionToStringVisitor.asString(isNullWhereExpr);

        List<String> pivotQs = Arrays.asList(pivotQ1, pivotQ2, pivotQ3);

        // Execute "crete tt" (table with one pivot row)
        SQLQueryAdapter q = new SQLQueryAdapter(ttCreate, errors);
        q.execute(state);

        SQLancerResultSet ttResult = null;
        SQLQueryAdapter ttSelect = new SQLQueryAdapter("select * from tt", errors);
        int nrow = 0;
        try {
            ttResult = ttSelect.executeAndGetLogged(state);

            if (ttResult == null) {
                // Possible bug here, investigate later
                throw new IgnoreMeException();
            }

            while (ttResult.next()) {
                nrow++;
            }
        } catch (Exception e) {
            // Possible bug here, investigate later
            throw new IgnoreMeException();
        } finally {
            if (ttResult != null && !ttResult.isClosed()) {
                ttResult.close();
            }
        }

        if (nrow == 0) {
            // If empty table is picked, we can't find a pivot row
            // Give up current check
            // TODO(datafusion): support empty tables
            throw new IgnoreMeException("Empty table is picked");
        }

        Node<DataFusionExpression> pivotPredicate = null;
        String pivotRow = "";
        for (int i = 0; i < pivotQs.size(); i++) {
            String pivotQ = pivotQs.get(i);
            SQLQueryAdapter qSelect = new SQLQueryAdapter(pivotQ, errors);
            SQLancerResultSet rs = null;
            try {
                rs = qSelect.executeAndGetLogged(state);
                if (rs == null) {
                    // Only one in 3 pivot query will return 1 row
                    continue;
                }

                int rowCount = 0;
                while (rs.next()) {
                    rowCount += 1;
                    for (int ii = 1; ii <= rs.rs.getMetaData().getColumnCount(); ii++) {
                        pivotRow += "[" + rs.getString(ii) + "]";
                    }
                    pivotPredicate = candidatePredicates.get(i);
                }

                dfAssert(rowCount <= 1, "Pivot row should be length of 1, got " + rowCount);

                if (rowCount == 1) {
                    break;
                }
            } catch (Exception e) {
                currentCheckLog.append(pivotQ).append("\n");
                currentCheckLog.append(e.getMessage()).append("\n").append(e.getCause()).append("\n");

                String fullErrorMessage = currentCheckLog.toString();
                state.dfLogger.appendToLog(DataFusionLogger.DataFusionLogType.ERROR, fullErrorMessage);

                throw new AssertionError(fullErrorMessage);
            } finally {
                if (rs != null && !rs.isClosed()) {
                    rs.close();
                }
            }
        }

        if (pivotPredicate == null) {
            // Sometimes all valid pivot queries failed
            // Potential bug, investigate later
            currentCheckLog.append("ALl pivot q failed! ").append(pivotQs).append("\n");
            throw new IgnoreMeException("All pivot queries failed " + pivotQs);
        }

        // ======================================================
        // Step 4:
        // Let's say in Step 3 we found the predicate is "Not (cc0 = cc2)"
        // Check if the pivot row can be find in
        // "select * from tt0, tt1 where Not(cc0 = cc2)"
        // Then we construct table ttt with above query
        // Finally join 'tt' and 'ttt' make sure one pivot row will be output
        // ======================================================
        DataFusionSelect selectAllRows = new DataFusionSelect();
        DataFusionFrom selectAllRowsFrom = new DataFusionFrom(randomSelect.tableList);
        selectAllRows.from = selectAllRowsFrom;
        selectAllRows.setWhereClause(pivotPredicate);

        List<DataFusionColumn> allSelectColumns = randomSelect.tableList.stream().flatMap(t -> t.getColumns().stream())
                .collect(Collectors.toList());
        // tt0.v0 as cc0, tt0.v1 as cc1, tt1.v0 as cc2
        List<String> allSelectExprs = allSelectColumns.stream().map(c -> c.getOrignalName() + " AS " + c.alias.get())
                .collect(Collectors.toList());
        resetColumnAlias(randomSelect.tableList);
        String selectAllRowsFetchColStr = String.join(", ", allSelectExprs);
        selectAllRows.setFetchColumnsString(selectAllRowsFetchColStr);

        String selectAllRowsString = DataFusionToStringVisitor.asString(selectAllRows);
        String tttCreate = "CREATE TABLE ttt AS\n" + selectAllRowsString;

        SQLQueryAdapter tttCreateStmt = new SQLQueryAdapter(tttCreate, errors);
        tttCreateStmt.execute(state);
        setColumnAlias(randomSelect.tableList);

        // ======================================================
        // Step 5:
        // Make sure the following query return 1 pivot row
        // Otherwise PQS oracle is violated
        // select * from tt join ttt
        // on
        // tt.cc0 is not distinct from ttt.cc0
        // and tt.cc1 is not distinct from ttt.cc1
        // and tt.cc2 is not distinct from ttt.cc2
        // ======================================================
        List<String> onConditions = allSelectColumns.stream()
                .map(c -> "(tt." + c.alias.get() + " IS NOT DISTINCT FROM ttt." + c.alias.get() + ")")
                .collect(Collectors.toList());
        String onCond = String.join("\nAND ", onConditions);
        String joinQuery = "SELECT COUNT(*) FROM tt JOIN ttt ON\n" + onCond;

        SQLQueryAdapter qJoin = new SQLQueryAdapter(joinQuery, errors);

        SQLancerResultSet rsFull = null;
        try {
            rsFull = qJoin.executeAndGetLogged(state);

            if (rsFull == null) {
                throw new IgnoreMeException("Join query returned no results: " + joinQuery);
            }

            String joinCount = "invalid";
            while (rsFull.next()) {
                joinCount = rsFull.getString(1);
            }

            if (joinCount.equals("0")) {
                String replay = DataFusionUtil.getReplay(state.getDatabaseName());
                StringBuilder errorLog = new StringBuilder().append("PQS oracle violated:\n").append("Found ")
                        .append(joinCount).append(" pivot rows:\n").append(" Pivot row: ").append(pivotRow).append("\n")
                        .append("Query to select pivot row: ").append(ttCreate).append("\n")
                        .append("Query to select all rows: ").append(tttCreate).append("\n").append("Join: ")
                        .append(joinQuery).append("\n").append(replay).append("\n");

                String errorString = errorLog.toString();
                String indentedErrorLog = errorString.replaceAll("(?m)^", " ");
                state.dfLogger.appendToLog(DataFusionLogger.DataFusionLogType.ERROR, errorString);

                throw new AssertionError("\n\n" + indentedErrorLog);
            } else if (!joinCount.matches("\\d+")) {
                // If joinCount is not a integer > 0, throw exception
                throw new IgnoreMeException("Join query returned invalid result: " + joinCount);
            }
        } catch (Exception e) {
            throw new IgnoreMeException("Failed to execute join query: " + joinQuery);
        } finally {
            if (rsFull != null && !rsFull.isClosed()) {
                rsFull.close();
            }
        }
    }
}
