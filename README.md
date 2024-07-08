# DataFusion-SQLancer
This is DataFusion's implementation of SQLancer on SQLancer's testing framework. See the original [README](https://github.com/sqlancer/sqlancer). 
`datafusion-sqlancer` has found ~10 [bugs](https://github.com/apache/datafusion/issues?q=is%3Aissue+label%3Abug+sqlancer+) with only a small subset of SQL features implemented.
# Overview
SQLancer (Synthesized Query Lancer) is a tool to automatically test Database Management Systems (DBMS) in order to find logic bugs in their implementation. It's a black box fuzzer which performs SQL-level testings.

SQLancer operates in the following two phases:

1. **Database and Query Generation:** Generating random database tables, and then generate complex queries using those tables
2. **Testing:** The goal of this phase is to detect the logic bugs **automatically** based on the generated database, using several **test oracles**.
	1. The weakest test oracle ensures that a query won't crash the `datafusion` engine. Since the query generator can construct chaotic queries, it's possible to detect crash bugs.
	2. `SQLancer` comes with several stronger test oracles, here is the example of one of those:
		```
		Non-optimizing Reference Engine Construction (NoREC)
		Randomly generated query(Q1): 
		    select * from t1 where v1 > 0;
		Mutated query(Q2): 
		    select v1 > 0 from t1;
		Consistency check:
		    The result size of Q1 should be equal to the number of `True` in Q2's output
		```
Above showed consistency check generated Q1 (very likely to be optimized by predicate pushdown), and Q2(hard to be optimized), such test oracle focuses on the correctness of the optimizer. There are 5 similar test oracles available to be implemented, those carefully designed checks make this testing framework really powerful.

More details about test oracles can be found in [SQLancer](https://github.com/sqlancer/sqlancer) page.

`DataFusion-SQLancer` can be viewed as an **extendible** SQL-level fuzz testing framework:
- It can generate random tables and queries that are supported by `DataFusion`.
- It's convenient to implement more test oracles.
	- One way is to come up with more `SQLancer` style oracles like making the above `Q2` into `select v1>= 0 from t1`, and check `Q2` 's result size is greater or equal to `Q1`'s
	- Another way is to change `DataFusion`'s configurations: for example `Q1`'s `t1` is in-memory table, and `Q2`'s `t2` can use `t1` backed by a `Parquet` file with same content. Or let `Q1` execute in multiple thread/partition, and `Q2` will be executed in single thread/partition.
# Getting Started

Requirements:
* Java 11 or above
* [Maven](https://maven.apache.org/) (`sudo apt install maven` on Ubuntu)

Download `datafusion-sqlancer`

```
git clone https://github.com/datafusion-contrib/datafusion-sqllancer
cd sqlancer
```

First start the `DataFusion` server

```
cd src/sqlancer/datafusion/server/datafusion_server
cargo run --release --features "datafusion_stable"
```

The following commands clone SQLancer, create a JAR, and start SQLancer to test SQLite using Non-optimizing Reference Engine Construction (NoREC):

```
mvn package -DskipTests
cd target
java --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED -jar sqlancer-*.jar --random-seed 0 --num-threads 1 --max-generated-databases 1 --num-tries 10 --num-queries 500 datafusion
```

If the execution prints progress information every five seconds, then the tool works as expected. Execution logs can be found at `target/logs/datafusion/`
# Testing Procedure
For execution `java --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED -jar sqlancer-*.jar --random-seed 0 --num-threads 1 --num-tries 10 --num-queries 500 datafusion`
1. It will perform 10(`--num-tries`) rounds of tests, at the beginning of each round, several random tables will be created.
2. In each round, 100(`--num-queries`) random query will be generated, each query will be test against random test oracles. If some query failed or 100 queries finished, `SQLancer` will go to next round and generate new databases.
## Testcase Generation
Table generator will first pick random number of tables, also random number of column (in random type), and get corresponding SQL `CREATE` statements to populate tables.
After, query generator know existing table schemas, it will generate a SQL query in a top-down way:
1. Generate a statement like `SELECT [expr1] FROM [expr2] WHERE [expr3]`, and with some probability, add extra `ORDER BY/JOIN/...` clause.
2. Generate expressions also in a top-down recursive way, for example in `WHERE` clause generator wants a boolean expression
	```
	generateExpression(BOOL)
	-> exprA==exprB // Pick one expression which will generate boolean result
	   -> generateExpression(numeric) for exprA
	      -> ...
	   -> generateExpression(numeric) for exprB
	      -> ...
	```
Notes for query generation:
- Expression generation is "typed", which means generator can specify what type to generate for a expression, but it can eventually generate a different type.
	- One reason is the generation implementation is "best effort" for simplicity, so the generation process can fail and produce a query that can't be executed on datafusion (which is fine: just continue with the next query)
	- Generator will ignore target type and pick another random type sometimes, to stress `DataFusion` engine to handle incorrect input argument types.(e.g. generate a numeric type in `WHERE` clause) 
- Expression generation is just a simple random walk on allowed syntax tree, some traditional fuzzing techniques is hard to apply in this context:
	- There is no feedback to guide the test case generation yet.
	- Generation is grammar-based, it can't generate bad input like `SELECT SELECT 1;`. (It's possible to explore this strategy in the future)
	- One way to let this approach attack more corner case is deliberately put more extreme values in the generation process:
		- e.g., Populating empty tables, insert more `NULL`, `INF`, `NaN` into tables etc.
# Code Architecture
- `DataFusion` server is adapted from [datafusion-examples](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/flight/flight_sql_server.rs), now only supported single thread `SQLancer` execution.
- `generateAndTestDatabase()` is a "driver" method to create random databases, generate queries, and finally test results.
- `DataFusionExpressionGenerator.java` includes top-down expression generation logic.
- `test/DataFusionNoRECOracle.java` contains the final result check for generated queries.
# Supported Features
- SQL Features: `SELECT`, `FROM`, `WHERE`
- Operators: Numeric, Comparison, Logical
- Scalar Functions: Numeric Scalar Functions
- SQLancer Test Oracles: `NoREC`, `TLP-Where`
# Bug Report
If any bug is found by `SQLancer`, it will print a full reproducer to terminal output, and also writes to `logs/datafusion_custom_log/error_report.log`.
1. Then, first verify the bug with latest `datafusion` main branch.
2. If bug is confirmed, try to reduce the bug and report it to the `datafusion` community.
3. If it's a false positive, feel free to open an issue here.



# Contribution
Upstream [SQLancer](https://github.com/sqlancer/sqlancer) repository have implementations of many DBMS, we plan to keep the development in `datafusion-contrib`, and sync with upstream once a while.

All kinds of contributions are welcome: feel free to open issues, discussions, or pull requests.