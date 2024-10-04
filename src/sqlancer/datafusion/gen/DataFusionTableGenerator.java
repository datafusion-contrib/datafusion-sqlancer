package sqlancer.datafusion.gen;

import java.util.Optional;
import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.datafusion.DataFusionProvider.DataFusionGlobalState;
import sqlancer.datafusion.DataFusionSchema.DataFusionDataType;
import sqlancer.datafusion.DataFusionSchema.DataFusionTable;
import sqlancer.datafusion.DataFusionSchema.DataFusionColumn;

public class DataFusionTableGenerator {

    // Randomly generate a query like 'create table t1 (v1 bigint, v2 boolean)'
    public SQLQueryAdapter getCreateStmt(DataFusionGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder();
        String tableName = globalState.getSchema().getFreeTableName();

        // Build "create table t1..." using sb
        sb.append("CREATE TABLE ");
        sb.append(tableName);
        sb.append("(");

        int colCount = (int) Randomly.getNotCachedInteger(1, 8);
        for (int i = 0; i < colCount; i++) {
            sb.append("v").append(i).append(" ").append(DataFusionDataType.getRandomWithoutNull().toString());

            if (i != colCount - 1) {
                sb.append(", ");
            }
        }

        sb.append(");");

        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    // Given a table t1, return create statement to generate t1_stringview
    // If t1 has no string column, return empty
    //
    // Query looks like (only v2 is TEXT column):
    // create table t1_stringview as
    // select v1, arrow_cast(v2, 'Utf8View') as v2 from t1;
    public Optional<SQLQueryAdapter> createStringViewTable(DataFusionGlobalState globalState, DataFusionTable table) {
        if (!table.getColumns().stream().anyMatch(c -> c.getType().equals(DataFusionDataType.STRING))) {
            return Optional.empty();
        }

        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ");
        sb.append(table.getName());
        sb.append("_stringview AS SELECT ");
        for (DataFusionColumn column : table.getColumns()) {
            String colName = column.getName();
            if (column.getType().equals(DataFusionDataType.STRING)) {
                // Found a TEXT column, cast it
                sb.append("arrow_cast(").append(colName).append(", 'Utf8View') as ").append(colName);
            } else {
                sb.append(colName);
            }

            // Join expressions with ','
            if (column != table.getColumns().get(table.getColumns().size() - 1)) {
                sb.append(", ");
            }
        }

        sb.append(" FROM ").append(table.getName()).append(";");

        return Optional.of(new SQLQueryAdapter(sb.toString(), new ExpectedErrors(), true));
    }

}
