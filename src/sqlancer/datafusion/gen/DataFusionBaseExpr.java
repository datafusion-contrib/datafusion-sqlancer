package sqlancer.datafusion.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.datafusion.DataFusionSchema.DataFusionDataType;

/*
    Notes for adding new `DataFusionBaseExpr` support:

    Expression ASTs are constructed with Node<> class, it can traverse expression and convert AST to String representation
    `DataFusionBaseExpr` implements `Operator<>` class, which is a field inside `Node<>` class, it includes operator properties like number of arguments, signature, or is this operator prefix/suffix, etc.

    To add new base expr (scalar functions, operators like '<<', 'AND' are all base expr):
    1. Add an enum variant to `DataFusionBaseExprType`
    2. Update `DataFusionBaseExprFactory.java`
    (If a function support different argument number, make a new entry for each one. e.g. round scalar function support round(3.14) / round(3.14, 1), so it should be enum FUNC_ROUND1, FUNC_ROUND2)
 */
public class DataFusionBaseExpr implements Operator {
    public String name;
    public int nArgs; // number of input arguments
    public DataFusionBaseExprCategory exprType;
    public List<DataFusionDataType> possibleReturnTypes;
    public List<ArgumentType> argTypes;
    public boolean isVariadic; // Function supports arbitrary number of arguments, if set to `true`, it will
    // override `nArgs`

    // Primary constructor
    DataFusionBaseExpr(String name, int nArgs, DataFusionBaseExprCategory exprCategory,
            List<DataFusionDataType> possibleReturnTypes, List<ArgumentType> argTypes, boolean isVariadic) {
        this.name = name;
        this.nArgs = nArgs;
        this.exprType = exprCategory;
        this.possibleReturnTypes = possibleReturnTypes;
        this.argTypes = argTypes;
        this.isVariadic = isVariadic;
    }

    // Overloaded constructor assuming 'isVariadic' is false
    DataFusionBaseExpr(String name, int nArgs, DataFusionBaseExprCategory exprCategory,
            List<DataFusionDataType> possibleReturnTypes, List<ArgumentType> argTypes) {
        this(name, nArgs, exprCategory, possibleReturnTypes, argTypes, false);
    }

    public static DataFusionBaseExpr createCommonNumericFuncSingleArg(String name) {
        return new DataFusionBaseExpr(name, 1, DataFusionBaseExprCategory.FUNC,
                Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                Arrays.asList(new ArgumentType.Fixed(
                        new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE)))));
    }

    public static DataFusionBaseExpr createCommonStringOperatorTwoArgs(String name) {
        return new DataFusionBaseExpr(name, 2, DataFusionBaseExprCategory.BINARY,
                Arrays.asList(DataFusionDataType.STRING),
                Arrays.asList(new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING))),
                        new ArgumentType.SameAsFirstArgType()));
    }

    public static DataFusionBaseExpr createCommonStringFuncOneStringArg(String name,
            List<DataFusionDataType> returnTypeList) {
        return new DataFusionBaseExpr(name, 1, DataFusionBaseExprCategory.FUNC, returnTypeList,
                Arrays.asList(new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING)))));
    }

    public static DataFusionBaseExpr createCommonStringFuncTwoStringArg(String name,
            List<DataFusionDataType> returnTypeList) {
        return new DataFusionBaseExpr(name, 2, DataFusionBaseExprCategory.FUNC, returnTypeList,
                Arrays.asList(new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING))),
                        new ArgumentType.SameAsFirstArgType()));
    }

    public static DataFusionBaseExpr createCommonNumericAggrFuncSingleArg(String name) {
        return new DataFusionBaseExpr(name, 1, DataFusionBaseExprCategory.AGGREGATE,
                Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                Arrays.asList(new ArgumentType.Fixed(
                        new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE)))));
    }

    public static DataFusionBaseExpr createCommonNumericAggrFuncTwoArg(String name) {
        return new DataFusionBaseExpr(name, 2, DataFusionBaseExprCategory.AGGREGATE,
                Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                Arrays.asList(
                        new ArgumentType.Fixed(
                                new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE))),
                        new ArgumentType.SameAsFirstArgType()));
    }

    public static DataFusionBaseExpr createCommonNumericFuncTwoArgs(String name) {
        return new DataFusionBaseExpr(name, 2, DataFusionBaseExprCategory.FUNC,
                Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                Arrays.asList(
                        new ArgumentType.Fixed(
                                new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE))),
                        new ArgumentType.Fixed(
                                new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE)))));
    }

    @Override
    public String getTextRepresentation() {
        return name;
    }

    @Override
    public String toString() {
        return name;
    }

    /*
     * Class/Enum for `DataFusionBaseExpr` fields
     */
    // Used to construct `src.common.ast.*Node`
    public enum DataFusionBaseExprCategory {
        UNARY_PREFIX, UNARY_POSTFIX, BINARY, FUNC, AGGREGATE, WINDOW
    }

    /*
     * Operators reference: https://datafusion.apache.org/user-guide/sql/operators.html Scalar functions:
     * https://datafusion.apache.org/user-guide/sql/scalar_functions.html
     */
    public enum DataFusionBaseExprType {
        // Null Operators
        IS_NULL, // IS NULL
        IS_NOT_NULL, // IS NOT NULL

        // Numeric Operators
        ADD, // 1 + 1
        SUB, // 1 - 1
        MULTIPLICATION, // 2 * 3
        DIVISION, // 8 / 4
        MODULO, // 5 % 3

        // Comparison Operators
        EQUAL, // 1 = 1
        EQUAL2, // 1 == 1
        NOT_EQUAL, // 1 != 2
        LESS_THAN, // 3 < 4
        LESS_THAN_OR_EQUAL_TO, // 3 <= 3
        GREATER_THAN, // 6 > 5
        GREATER_THAN_OR_EQUAL_TO, // 5 >= 5

        // Distinctness operators
        IS_DISTINCT_FROM, // 0 IS DISTINCT FROM NULL
        IS_NOT_DISTINCT_FROM, // NULL IS NOT DISTINCT FROM NULL

        // Pattern matching expressions
        LIKE, // 'foo' like 'fo'
        NOT_LIKE, ILIKE, NOT_ILIKE,

        // Regular expression match operators
        REGEX_MATCH, // 'datafusion' ~ '^datafusion(-cli)*'
        REGEX_CASE_INSENSITIVE_MATCH, // 'datafusion' ~* '^DATAFUSION(-cli)*'
        NOT_REGEX_MATCH, // 'datafusion' !~ '^DATAFUSION(-cli)*'
        NOT_REGEX_CASE_INSENSITIVE_MATCH, // 'datafusion' !~* '^DATAFUSION(-cli)+'

        // Like pattern match operators
        LIKE_MATCH, // 'datafusion' ~~ 'dat_f%n'
        CASE_INSENSITIVE_LIKE_MATCH, // 'datafusion' ~~* 'Dat_F%n'
        NOT_LIKE_MATCH, // 'datafusion' !~~ 'Dat_F%n'
        NOT_CASE_INSENSITIVE_LIKE_MATCH, // 'datafusion' !~~* 'Dat%F_n'

        // Logical Operators
        AND, // true and true
        OR, // true or false

        // Bitwise Operators
        BITWISE_AND, // 5 & 3
        BITWISE_OR, // 5 | 3
        BITWISE_XOR, // 5 ^ 3
        BITWISE_SHIFT_RIGHT, // 5 >> 3
        BITWISE_SHIFT_LEFT, // 5 << 3

        // Other operators
        STRING_CONCATENATION, // 'Hello, ' || 'DataFusion!'
        // ARRAY_CONTAINS, // make_array(1,2,3) @> make_array(1,3)
        // ARRAY_IS_CONTAINED_BY, // make_array(1,3) <@ make_array(1,2,3)

        // Unary Prefix Operators
        NOT, // NOT true
        PLUS, // +7
        MINUS, // -3

        /*
         * Scalar Functions
         */

        // Math Functions
        FUNC_ABS, // abs(-10)
        FUNC_ACOS, // acos(1)
        FUNC_ACOSH, // acosh(10)
        FUNC_ASIN, // asin(1)
        FUNC_ASINH, // asinh(1)
        FUNC_ATAN, // atan(1)
        FUNC_ATANH, // atanh(0.5)
        FUNC_ATAN2, // atan2(10, 10)
        FUNC_CBRT, // cbrt(27)
        FUNC_CEIL, // ceil(9.2)
        FUNC_COS, // cos(π/3)
        FUNC_COSH, // cosh(0)
        FUNC_DEGREES, // degrees(π)
        FUNC_EXP, // exp(1)
        FUNC_FACTORIAL, // factorial(5)
        FUNC_FLOOR, // floor(3.7)
        FUNC_GCD, // gcd(8, 12)
        FUNC_ISNAN, // isnan(NaN)
        FUNC_ISZERO, // iszero(0.0)
        FUNC_LCM, // lcm(5, 15)
        FUNC_LN, // ln(1)
        FUNC_LOG, // log(100)
        FUNC_LOG_WITH_BASE, // log(10, 100)
        FUNC_LOG10, // log10(100)
        FUNC_LOG2, // log2(32)
        FUNC_NANVL, // nanvl(NaN, 3)
        FUNC_PI, // pi()
        FUNC_POW, // pow(2, 3)
        FUNC_POWER, // power(2, 3)
        FUNC_RADIANS, // radians(180)
        // FUNC_RANDOM, // random() disabled because it's non-deterministic
        FUNC_ROUND, // round(3.14159)
        FUNC_ROUND_WITH_DECIMAL, // round(3.14159, 2)
        FUNC_SIGNUM, // signum(-10)
        FUNC_SIN, // sin(π/2)
        FUNC_SINH, // sinh(1)
        FUNC_SQRT, // sqrt(16)
        FUNC_TAN, // tan(π/4)
        FUNC_TANH, // tanh(1)
        FUNC_TRUNC, // trunc(3.14159)
        FUNC_TRUNC_WITH_DECIMAL, // trunc(3.14159, 2)

        // Conditional Functions
        FUNC_COALESCE, // coalesce(NULL, 'default value')
        FUNC_NULLIF, // nullif('value', 'value')
        FUNC_NVL, // nvl(NULL, 'default value')
        FUNC_NVL2, // nvl2('not null', 'return if not null', 'return if null')
        FUNC_IFNULL, // ifnull(NULL, 'default value')

        // String Functions
        // TODO(datafusion) lpad('foo', 1e100) takes forever, we can let server stop a query if its been running for too
        // long
        // String Functions - return numeric
        FUNC_ASCII, // ascii('string')
        FUNC_LENGTH, // length('string')
        FUNC_CHAR_LENGTH, // char_length('string')
        FUNC_CHARACTER_LENGTH, // character_length('string')
        FUNC_BIT_LENGTH, // bit_length('string')
        FUNC_CHR, // chr(code)
        FUNC_INSTR, // instr('string', 'substring')
        FUNC_STRPOS, // strpos('string', 'substring')
        FUNC_LEVENSHTEIN, // levenshtein('string1', 'string2')
        FUNC_FIND_IN_SET, // find_in_set('b', 'a,b,c,d')
        // String Functions - return String
        FUNC_INITCAP, // initcap('string')
        FUNC_LOWER, // lower('string')
        FUNC_UPPER, // upper('string')
        FUNC_OCTET_LENGTH, // octet_length('string')
        FUNC_BTRIM, // btrim(' string ')
        FUNC_BTRIM2, // btrim('--string--', '-')
        FUNC_TRIM, // trim('string')
        FUNC_TRIM2, // trim('string', 'trim_chars')
        FUNC_LTRIM, // ltrim(' string ')
        FUNC_LTRIM2, // ltrim('--string-', '-')
        FUNC_RTRIM, // rtrim('string ')
        FUNC_RTRIM2, // rtrim('-string--', '-')
        FUNC_LEFT, // left('string', n)
        FUNC_RIGHT, // right('string', n)

        FUNC_CONCAT, // concat('string1', 'string2', ...)
        FUNC_CONCAT_WS, // concat_ws('separator', 'string1', 'string2', ...)

        // FUNC_LPAD, // lpad('string', length)
        // FUNC_LPAD2, // lpad('string', length, 'pad_string')
        // FUNC_RPAD, // rpad('string', length)
        // FUNC_RPAD2, // rpad('string', length, 'pad_string')

        // FUNC_REPEAT, // repeat('string', n)
        FUNC_REPLACE, // replace('string', 'search', 'replacement')
        FUNC_REVERSE, // reverse('string')
        FUNC_SPLIT_PART, // split_part('foo-bar-baz', '-', 3)

        FUNC_SUBSTR, // substr('string', start_pos)
        FUNC_SUBSTR2, // substr('string', start_pos[, length])
        FUNC_SUBSTRING, // substring('string', start_pos)
        FUNC_SUBSTRING2, // substring('string', start_pos[, length])
        FUNC_TRANSLATE, // translate('hello-world', '-', '--')

        FUNC_TO_HEX, // to_hex(number)
        // FUNC_UUID, // uuid()
        FUNC_SUBSTR_INDEX, // substr_index('string', 'delimiter', count)
        FUNC_SUBSTRING_INDEX, // substring_index('string', 'delimiter', count)

        // String Functions - Return boolean
        FUNC_ENDS_WITH, // ends_with('string', 'suffix')
        FUNC_STARTS_WITH, // starts_with('string', 'prefix')

        // FUNC_OVERLAY, // overlay('string' placing 'substr' from position [for count])
        // TODO(datafusion) generate valid flags for regexp functions
        FUNC_REGEXP_LIKE, // regexp_like('aBc', '(b|d)')
        FUNC_REGEXP_LIKE2, // regexp_like('aBc', '(b|d)', 'i')
        FUNC_REGEXP_MATCH, // regexp_match('aBc', '(b|d)')
        FUNC_REGEXP_MATCH2, // regexp_match('aBc', '(b|d)', 'i')
        FUNC_REGEXP_REPLACE, // regexp_replace('aBc', '(b|d)')
        FUNC_REGEXP_REPLACE2, // regexp_replace('aBc', '(b|d)', 'Ab\\1a', 'i')

        // Time and Date Functions

        // Array Functions

        // Struct Functions

        // Hashing Functions

        // Other Functions

        // Aggregate Functions (General)
        AGGR_MIN, AGGR_MAX, AGGR_SUM, AGGR_AVG, AGGR_COUNT, AGGR_BIT_AND, AGGR_BIT_OR, AGGR_BIT_XOR, AGGR_BOOL_AND,
        AGGR_BOOL_OR, AGGR_MEAN, AGGR_MEDIAN, AGGR_FIRST_VALUE, AGGR_LAST_VALUE,
        // Aggregate Functiosn (Statistical)
        AGGR_CORR, // corr(v1, v2)
        AGGR_COVAR, // covar(v1, v2)
        AGGR_POP, // covar_pop(v1, v2)
        AGGR_COVAR_SAMP, // covar_samp(v1, v2)
        AGGR_STDDEV, // stddev(v)
        AGGR_STDDEV_POP, // stddev_pop(v)
        AGGR_STDDEV_SAMP, // stddev_samp(v)
        AGGR_VAR, // var(v)
        AGGR_VAR_POP, // var_pop(v)
        AGGR_VAR_SAMP, // var_samp(v)
        AGGR_REGR_AVGX, // regr_avgx(y, x)
        AGGR_REGR_AVGY, // regr_avgy(y, x)
        AGGR_REGR_COUNT, // regr_count(y, x)
        AGGR_REGR_INTERCEPT, // regr_intercept(y, x)
        AGGR_REGR_R2, // regr_r2(y, x)
        AGGR_REGR_SLOPE, // regr_slope(y, x)
        AGGR_REGR_SXX, // regr_sxx(x)
        AGGR_REGR_SYY, // regr_syy(y)
        AGGR_REGR_SXY, // regr_sxy(x, y)
        // Aggregate Functions (Approximate)
        AGGR_APPROX_DISTINCT, // approx_distinct(expression)
        AGGR_APPROX_MEDIAN, // approx_median(expression)
        AGGR_APPROX_PERCENTILE_CONT, // approx_percentile_cont(expression, percentile)
        AGGR_APPROX_PERCENTILE_CONT2, // approx_percentile_cont(expression, percentile, centroids)
        AGGR_APPROX_PERCENTILE_CONT_WITH_WEIGHT, // approx_percentile_cont_with_weight(expression, weight, percentile)

        // Array Aggregate functions

        // Window Functions
        WINDOW_RANK, // rank()
        WINDOW_ROW_NUMBER, // row_number()
        WINDOW_DENSE_RANK, // dense_rank()
        WINDOW_NTILE, // ntile(2) - divides the partition into 2 groups
        WINDOW_CUME_DIST, // cume_dist() - Relative rank of the current row: WINDOW(number of rows preceding or peer
                          // with current row) / (total rows)
        WINDOW_PERCENT_RANK, // percent_rank() - Relative rank of the current row:WINDOW (rank - 1) / (total rows - 1)
        WINDOW_LAG1, // 1 arg version of lag(expression, offset, default) - Returns value from rows WINDOWbefore the
                     // current row within the partition
        WINDOW_LAG2, // 2 args version
        WINDOW_LAG3, // 3 args version
        WINDOW_LEAD1, // 1 arg version of lead(expression, offset, default) - Returns value from rows after the current
                      // row within the partition
        WINDOW_LEAD2, // 2 args version
        WINDOW_LEAD3, // 3 args version
        WINDOW_FIRST_VALUE, // first_value(expression) - Returns value from the first row of the window frame
        WINDOW_LAST_VALUE, // last_value(expression) - Returns value from the last row of the window frame
        WINDOW_NTH_VALUE, // nth_value(expression, n) - Returns value from the nth row of the window frame
    }

    /*
     * Because expressions are constructed in a top-down way, we have to infer argument type given return type. For each
     * arg, if its corresponding element is `SameAsReturnType`, it should be the same as the type of expression's
     * evaluated value. Else, it should be specific `DataFusionDataType`
     *
     * e.g. let's say we're generating a round(num, digit) of double type, its `argTypes` is: Arrays.asList( new
     * ArgumentType.SameAsReturnType(), // First arg type as return type new ArgumentType.Fixed(new
     * ArrayList<>(Array.asList(DataFusionDataType.INT)) // Second arg always Integer ) it means: its first argument
     * should be the same as returned type (double), and the second arg should always be Int.
     *
     * Random expression generator's policy: SameAsReturnType -> generate an expr with the same type as its return type
     * SameAsReturnType -> generate an expr with the same type as its 1st arg type Fixed(type1, type2, ... typeN) ->
     * randomly choose a possible type (It will also generate completely random type/null ~10%)
     *
     * Note this defination is not comprehensive for native `DataFusion` types. It's just for simplicity and should
     * cover most common cases
     */
    public abstract static class ArgumentType {
        private ArgumentType() {
        }

        public static class SameAsReturnType extends ArgumentType {
        }

        public static class SameAsFirstArgType extends ArgumentType {
        }

        public static class Fixed extends ArgumentType {
            public List<DataFusionDataType> fixedType; // It's a list to support different possible arg types.

            public Fixed(List<DataFusionDataType> fixedType) {
                this.fixedType = fixedType;
            }

            public List<DataFusionDataType> getType() {
                return fixedType;
            }
        }
    }
}
