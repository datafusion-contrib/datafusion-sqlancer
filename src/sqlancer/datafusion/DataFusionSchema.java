package sqlancer.datafusion;

import static sqlancer.datafusion.DataFusionUtil.dfAssert;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTable;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.TableIndex;
import sqlancer.datafusion.DataFusionProvider.DataFusionGlobalState;
import sqlancer.datafusion.DataFusionSchema.DataFusionTable;
import sqlancer.datafusion.ast.DataFusionConstant;
import sqlancer.datafusion.ast.DataFusionExpression;

public class DataFusionSchema extends AbstractSchema<DataFusionGlobalState, DataFusionTable> {

    public DataFusionSchema(List<DataFusionTable> databaseTables) {
        super(databaseTables);
    }

    // update existing tables in DB by query again
    // (like `show tables;`)
    //
    // This function also setup table<->column reference pointers
    // and equivalent tables(see `DataFusionTable.equivalentTables)
    public static DataFusionSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        List<DataFusionTable> databaseTables = new ArrayList<>();
        List<String> tableNames = getTableNames(con);

        for (String tableName : tableNames) {
            List<DataFusionColumn> databaseColumns = getTableColumns(con, tableName);
            boolean isView = tableName.startsWith("v");
            DataFusionTable t = new DataFusionTable(tableName, databaseColumns, isView);
            for (DataFusionColumn c : databaseColumns) {
                c.setTable(t);
            }

            databaseTables.add(t);
        }

        // Setup equivalent tables
        // For example, now we have t1, t1_csv, t1_parquet, t2_csv, t2_parquet
        // t1's equivalent tables: t1, t1_csv, t1_parquet
        // t2_csv's equivalent tables: t2_csv, t2_parquet
        // ...
        //
        // It can be assumed that:
        // base table names are like t1, t2, ...
        // equivalent tables are like t1_csv, t1_parquet, ...
        for (DataFusionTable t : databaseTables) {
            String baseTableName = t.getName().split("_")[0];
            String patternString = "^" + baseTableName + "(_.*)?$"; // t1 or t1_*
            Pattern pattern = Pattern.compile(patternString);

            t.equivalentTables = databaseTables.stream().filter(table -> pattern.matcher(table.getName()).matches())
                    .map(DataFusionTable::getName).collect(Collectors.toList());
        }

        return new DataFusionSchema(databaseTables);
    }

    private static List<String> getTableNames(SQLConnection con) throws SQLException {
        List<String> tableNames = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("select table_name " + "from information_schema.tables "
                    + "where table_schema='public'" + "order by table_name; ")) {
                while (rs.next()) {
                    tableNames.add(rs.getString(1));
                }
            }
        }
        return tableNames;
    }

    private static List<DataFusionColumn> getTableColumns(SQLConnection con, String tableName) throws SQLException {
        List<DataFusionColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(
                    String.format("select * from information_schema.columns where table_name = '%s';", tableName))) {
                while (rs.next()) {
                    String columnName = rs.getString("column_name");
                    String dataType = rs.getString("data_type");
                    boolean isNullable = rs.getString("is_nullable").contentEquals("YES");

                    DataFusionColumn c = new DataFusionColumn(columnName,
                            DataFusionDataType.parseFromDataFusionCatalog(dataType), isNullable);
                    columns.add(c);
                }
            }
        }

        return columns;
    }

    /*
     * When adding a new type: 1. Update all methods inside this enum 2. Update all `DataFusionBaseExpr`'s signature, if
     * it can support new type (in `DataFusionBaseExprFactory.java`)
     *
     * Types are 'SQL DataType' in DataFusion's documentation
     * https://datafusion.apache.org/user-guide/sql/data_types.html
     */
    public enum DataFusionDataType {

        STRING, BIGINT, DOUBLE, BOOLEAN, NULL;

        public static DataFusionDataType getRandomWithoutNull() {
            DataFusionDataType dt;
            do {
                dt = Randomly.fromOptions(values());
            } while (dt == DataFusionDataType.NULL);
            return dt;
        }

        public boolean isNumeric() {
            return this == BIGINT || this == DOUBLE;
        }

        // How to parse type in DataFusion's catalog to `DataFusionDataType`
        // As displayed in:
        // create table t1(v1 int, v2 bigint);
        // select table_name, column_name, data_type from information_schema.columns;
        public static DataFusionDataType parseFromDataFusionCatalog(String typeString) {
            switch (typeString) {
            case "Int64":
                return DataFusionDataType.BIGINT;
            case "Float64":
                return DataFusionDataType.DOUBLE;
            case "Boolean":
                return DataFusionDataType.BOOLEAN;
            case "Utf8":
                return DataFusionDataType.STRING;
            case "Utf8View":
                return DataFusionDataType.STRING;
            default:
                dfAssert(false, "Uncovered branch typeString: " + typeString);
            }

            dfAssert(false, "Unreachable. All branches should be eovered");
            return null;
        }

        // TODO(datafusion) lots of hack here, should build our own Randomly later
        public Node<DataFusionExpression> getRandomConstant(DataFusionGlobalState state) {
            if (Randomly.getBooleanWithSmallProbability()) {
                return DataFusionConstant.createNullConstant();
            }
            switch (this) {
            case BIGINT:
                long randInt = Randomly.getBoolean() ? state.getRandomly().getInteger()
                        : state.getRandomly().getInteger(-5, 5);
                return DataFusionConstant.createIntConstant(randInt);
            case BOOLEAN:
                return new DataFusionConstant.DataFusionBooleanConstant(Randomly.getBoolean());
            case DOUBLE:
                if (Randomly.getBoolean()) {
                    if (Randomly.getBoolean()) {
                        Double randomDouble = state.getRandomly().getDouble(); // [0.0, 1.0);
                        Double scaledDouble = (randomDouble - 0.5) * 2 * Double.MAX_VALUE;
                        return new DataFusionConstant.DataFusionDoubleConstant(scaledDouble);
                    }
                    String doubleStr = Randomly.fromOptions("'NaN'::Double", "'+Inf'::Double", "'-Inf'::Double", "-0.0",
                            "+0.0");
                    return new DataFusionConstant.DataFusionDoubleConstant(doubleStr);
                }

                return new DataFusionConstant.DataFusionDoubleConstant(state.getRandomly().getDouble());
            case NULL:
                return DataFusionConstant.createNullConstant();
            case STRING:
                return new DataFusionConstant.DataFusionStringConstant(state.getRandomly().getString());
            default:
                dfAssert(false, "Unreachable. All branches should be eovered");
            }

            dfAssert(false, "Unreachable. All branches should be eovered");
            return DataFusionConstant.createNullConstant();
        }
    }

    public static class DataFusionColumn extends AbstractTableColumn<DataFusionTable, DataFusionDataType> {

        private final boolean isNullable;
        public Optional<String> alias;

        public DataFusionColumn(String name, DataFusionDataType columnType, boolean isNullable) {
            super(name, null, columnType);
            this.isNullable = isNullable;
            this.alias = Optional.empty();
        }

        public boolean isNullable() {
            return isNullable;
        }

        public String getOrignalName() {
            return getTable().getName() + "." + getName();
        }

        @Override
        public String getFullQualifiedName() {
            if (getTable() == null) {
                return getName();
            } else {
                if (alias.isPresent()) {
                    return alias.get();
                } else {
                    return getTable().getName() + "." + getName();
                }
            }
        }
    }

    public static class DataFusionTable
            extends AbstractRelationalTable<DataFusionColumn, TableIndex, DataFusionGlobalState> {
        // There might exist multiple logically equivalent tables with
        // different physical format.
        // e.g. t1_csv, t1_parquet, ...
        //
        // When generating random query, it's possible to randomly pick one
        // of them for stronger randomization.
        public List<String> equivalentTables;

        // Pick a random equivalent table name
        // This can be used when generating differential queries
        public Optional<String> currentEquivalentTableName;

        // For example in query `select * from t1 as tt1, t1 as tt2`
        // `tt1` is the alias for the first occurance of `t1`
        public Optional<String> alias;

        public DataFusionTable(String tableName, List<DataFusionColumn> columns, boolean isView) {
            super(tableName, columns, Collections.emptyList(), isView);
        }

        public String getNotAliasedName() {
            if (currentEquivalentTableName != null && currentEquivalentTableName.isPresent()) {
                // In case setup is not done yet
                return currentEquivalentTableName.get();
            } else {
                return super.getName();
            }
        }

        // TODO(datafusion) Now implementation is hacky, should send a patch
        // to core to support this
        @Override
        public String getName() {
            // Before setup equivalent tables, we use the original table name
            // Setup happens in `fromConnection()`
            if (equivalentTables == null || currentEquivalentTableName == null) {
                return super.getName();
            }

            if (alias.isPresent()) {
                return alias.get();
            } else {
                return currentEquivalentTableName.get();
            }
        }

        public void pickAnotherEquivalentTableName() {
            dfAssert(!equivalentTables.isEmpty(), "equivalentTables should not be empty");
            currentEquivalentTableName = Optional.of(Randomly.fromList(equivalentTables));
        }

        public static List<DataFusionColumn> getAllColumns(List<DataFusionTable> tables) {
            return tables.stream().map(AbstractTable::getColumns).flatMap(List::stream).collect(Collectors.toList());
        }

        public static List<DataFusionColumn> getRandomColumns(List<DataFusionTable> tables) {
            if (Randomly.getBooleanWithRatherLowProbability()) {
                return Arrays.asList(new DataFusionColumn("*", DataFusionDataType.NULL, true));
            }

            List<DataFusionColumn> allColumns = getAllColumns(tables);

            return Randomly.nonEmptySubset(allColumns);
        }
    }

}
