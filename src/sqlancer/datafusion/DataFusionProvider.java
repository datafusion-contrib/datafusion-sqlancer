package sqlancer.datafusion;

import static sqlancer.datafusion.DataFusionUtil.dfAssert;
import static sqlancer.datafusion.DataFusionUtil.displayTables;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import com.google.auto.service.AutoService;

import sqlancer.DatabaseProvider;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.SQLGlobalState;
import sqlancer.SQLProviderAdapter;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.datafusion.DataFusionProvider.DataFusionGlobalState;
import sqlancer.datafusion.DataFusionSchema.DataFusionTable;
import sqlancer.datafusion.DataFusionUtil.DataFusionInstanceID;
import sqlancer.datafusion.DataFusionUtil.DataFusionLogger;
import sqlancer.datafusion.gen.DataFusionInsertGenerator;
import sqlancer.datafusion.gen.DataFusionTableGenerator;

@AutoService(DatabaseProvider.class)
public class DataFusionProvider extends SQLProviderAdapter<DataFusionGlobalState, DataFusionOptions> {

    public DataFusionProvider() {
        super(DataFusionGlobalState.class, DataFusionOptions.class);
    }

    // Basic tables generated are DataFusion memory tables (named t1, t2, ...)
    // Equivalent table can be backed by different physical implementation
    // which will be named like t1_stringview, t2_parquet, etc.
    //
    // e.g. t1 and t1_stringview are logically equivalent table, but backed by
    // different physical representation
    //
    // This helps to do more metamorphic testing on tables, for example
    // `select * from t1` and `select * from t1_stringview` should give same
    // result
    //
    // Supported physical implementation for tables:
    // 1. Memory table (t1)
    // 2. Memory table use StringView for TEXT columns (t1_stringview)
    // Note: It's possible only convert random TEXT columns to StringView
    @Override
    public void generateDatabase(DataFusionGlobalState globalState) throws Exception {
        // Create base tables
        // ============================

        int tableCount = Randomly.fromOptions(1, 2, 3, 4);
        for (int i = 0; i < tableCount; i++) {
            SQLQueryAdapter queryCreateRandomTable = new DataFusionTableGenerator().getCreateStmt(globalState);
            queryCreateRandomTable.execute(globalState);
            globalState.updateSchema();
            globalState.dfLogger.appendToLog(DataFusionLogger.DataFusionLogType.DML,
                    queryCreateRandomTable.toString() + "\n");
        }

        // Now only `INSERT` DML is supported
        // If more DMLs are added later, should use`StatementExecutor` instead
        // (see DuckDB's implementation for reference)

        // Generating rows in base tables (t1, t2, ... not include t1_stringview, etc.)
        // ============================

        globalState.updateSchema();
        List<DataFusionTable> allBaseTables = globalState.getSchema().getDatabaseTables();
        List<String> allBaseTablesName = allBaseTables.stream().map(DataFusionTable::getName)
                .collect(Collectors.toList());
        if (allBaseTablesName.isEmpty()) {
            dfAssert(false, "Generate Database failed.");
        }

        // Randomly insert some data into existing tables
        for (DataFusionTable table : allBaseTables) {
            int nInsertQuery = globalState.getRandomly().getInteger(0, globalState.getOptions().getMaxNumberInserts());

            for (int i = 0; i < nInsertQuery; i++) {
                SQLQueryAdapter insertQuery = null;
                try {
                    insertQuery = DataFusionInsertGenerator.getQuery(globalState, table);
                } catch (IgnoreMeException e) {
                    // Only for special case: table has 0 column
                    continue;
                }

                insertQuery.execute(globalState);
                globalState.dfLogger.appendToLog(DataFusionLogger.DataFusionLogType.DML, insertQuery.toString() + "\n");
            }
        }

        // Construct mutated tables like t1_stringview, etc.
        // ============================
        for (DataFusionTable table : allBaseTables) {
            Optional<SQLQueryAdapter> queryCreateStringViewTable = new DataFusionTableGenerator()
                    .createStringViewTable(globalState, table);
            if (queryCreateStringViewTable.isPresent()) {
                queryCreateStringViewTable.get().execute(globalState);
                globalState.dfLogger.appendToLog(DataFusionLogger.DataFusionLogType.DML,
                        queryCreateStringViewTable.get().toString() + "\n");
            }
        }
        globalState.updateSchema();
        List<DataFusionTable> allTables = globalState.getSchema().getDatabaseTables();
        List<String> allTablesName = allTables.stream().map(DataFusionTable::getName).collect(Collectors.toList());

        // TODO(datafusion) add `DataFUsionLogType.STATE` for this whole db state log
        if (globalState.getDbmsSpecificOptions().showDebugInfo) {
            System.out.println(displayTables(globalState, allTablesName));
        }
    }

    @Override
    public SQLConnection createDatabase(DataFusionGlobalState globalState) throws SQLException {
        if (globalState.getDbmsSpecificOptions().showDebugInfo) {
            System.out.println("A new database get created!\n");
        }
        Properties props = new Properties();
        props.setProperty("UseEncryption", "false");
        // must set 'user' and 'password' to trigger server 'do_handshake()'
        props.setProperty("user", "foo");
        props.setProperty("password", "bar");
        props.setProperty("create", globalState.getDatabaseName()); // Hack: use this property to let DataFusion server
        // clear the current context
        String url = "jdbc:arrow-flight-sql://127.0.0.1:50051";
        Connection connection = DriverManager.getConnection(url, props);

        return new SQLConnection(connection);
    }

    @Override
    public String getDBMSName() {
        return "datafusion";
    }

    // If run SQLancer with multiple thread
    // Each thread's instance will have its own `DataFusionGlobalState`
    // It will store global states including:
    // JDBC connection to DataFusion server
    // Logger for this thread
    public static class DataFusionGlobalState extends SQLGlobalState<DataFusionOptions, DataFusionSchema> {
        public DataFusionLogger dfLogger;
        DataFusionInstanceID id;

        public DataFusionGlobalState() {
            // HACK: test will only run in spawned thread, not main thread
            // this way redundant logger files won't be created
            if (Thread.currentThread().getName().equals("main")) {
                return;
            }

            id = new DataFusionInstanceID(Thread.currentThread().getName());
            try {
                dfLogger = new DataFusionLogger(this, id);
            } catch (Exception e) {
                throw new IgnoreMeException();
            }
        }

        @Override
        protected DataFusionSchema readSchema() throws SQLException {
            return DataFusionSchema.fromConnection(getConnection(), getDatabaseName());
        }
    }
}
