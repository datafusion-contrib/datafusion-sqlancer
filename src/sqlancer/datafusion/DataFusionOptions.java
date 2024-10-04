package sqlancer.datafusion;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;
import sqlancer.OracleFactory;
import sqlancer.common.oracle.TestOracle;
import sqlancer.datafusion.DataFusionOptions.DataFusionOracleFactory;
import sqlancer.datafusion.DataFusionProvider.DataFusionGlobalState;
import sqlancer.datafusion.test.DataFusionNoCrashAggregate;
import sqlancer.datafusion.test.DataFusionNoCrashWindow;
import sqlancer.datafusion.test.DataFusionNoRECOracle;
import sqlancer.datafusion.test.DataFusionQueryPartitioningAggrTester;
import sqlancer.datafusion.test.DataFusionQueryPartitioningHavingTester;
import sqlancer.datafusion.test.DataFusionQueryPartitioningWhereTester;

@Parameters(commandDescription = "DataFusion")
public class DataFusionOptions implements DBMSSpecificOptions<DataFusionOracleFactory> {
    @Parameter(names = "--debug-info", description = "Show debug messages related to DataFusion", arity = 0)
    public boolean showDebugInfo;

    @Override
    public List<DataFusionOracleFactory> getTestOracleFactory() {
        return Arrays.asList(
                DataFusionOracleFactory.NO_CRASH_WINDOW,
                DataFusionOracleFactory.NO_CRASH_AGGREGATE,
                DataFusionOracleFactory.NOREC,
                DataFusionOracleFactory.QUERY_PARTITIONING_WHERE);
        // DataFusionOracleFactory.QUERY_PARTITIONING_AGGREGATE
        // DataFusionOracleFactory.QUERY_PARTITIONING_HAVING);
    }

    public enum DataFusionOracleFactory implements OracleFactory<DataFusionGlobalState> {
        NOREC {
            @Override
            public TestOracle<DataFusionGlobalState> create(DataFusionGlobalState globalState) throws SQLException {
                return new DataFusionNoRECOracle(globalState);
            }
        },
        QUERY_PARTITIONING_WHERE {
            @Override
            public TestOracle<DataFusionGlobalState> create(DataFusionGlobalState globalState) throws SQLException {
                return new DataFusionQueryPartitioningWhereTester(globalState);
            }
        },
        QUERY_PARTITIONING_HAVING {
            @Override
            public TestOracle<DataFusionGlobalState> create(DataFusionGlobalState globalState) throws SQLException {
                return new DataFusionQueryPartitioningHavingTester(globalState);
            }
        },
        QUERY_PARTITIONING_AGGREGATE {
            @Override
            public TestOracle<DataFusionGlobalState> create(DataFusionGlobalState globalState) throws SQLException {
                return new DataFusionQueryPartitioningAggrTester(globalState);
            }
        },
        NO_CRASH_AGGREGATE {
            @Override
            public TestOracle<DataFusionGlobalState> create(DataFusionGlobalState globalState) throws SQLException {
                return new DataFusionNoCrashAggregate(globalState);
            }
        },
        NO_CRASH_WINDOW {
            @Override
            public TestOracle<DataFusionGlobalState> create(DataFusionGlobalState globalState) throws SQLException {
                return new DataFusionNoCrashWindow(globalState);
            }
        }
    }

}
