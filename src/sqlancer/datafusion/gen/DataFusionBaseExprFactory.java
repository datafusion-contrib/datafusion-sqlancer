package sqlancer.datafusion.gen;

import static sqlancer.datafusion.DataFusionUtil.dfAssert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.datafusion.DataFusionSchema.DataFusionDataType;
import sqlancer.datafusion.gen.DataFusionBaseExpr.ArgumentType;
import sqlancer.datafusion.gen.DataFusionBaseExpr.DataFusionBaseExprCategory;
import sqlancer.datafusion.gen.DataFusionBaseExpr.DataFusionBaseExprType;

public final class DataFusionBaseExprFactory {
    private DataFusionBaseExprFactory() {
        dfAssert(false, "Utility class cannot be instantiated");
    }

    public static DataFusionBaseExpr createExpr(DataFusionBaseExprType type) {
        switch (type) {
        case IS_NULL:
            return new DataFusionBaseExpr("IS NULL", 1, DataFusionBaseExprCategory.UNARY_POSTFIX,
                    Arrays.asList(DataFusionDataType.BOOLEAN),
                    Arrays.asList(new ArgumentType.Fixed(
                            new ArrayList<>(Arrays.asList(DataFusionDataType.STRING, DataFusionDataType.BOOLEAN,
                                    DataFusionDataType.DOUBLE, DataFusionDataType.BIGINT, DataFusionDataType.NULL)))));
        case IS_NOT_NULL:
            return new DataFusionBaseExpr("IS NOT NULL", 1, DataFusionBaseExprCategory.UNARY_POSTFIX,
                    Arrays.asList(DataFusionDataType.BOOLEAN),
                    Arrays.asList(new ArgumentType.Fixed(
                            new ArrayList<>(Arrays.asList(DataFusionDataType.STRING, DataFusionDataType.BOOLEAN,
                                    DataFusionDataType.DOUBLE, DataFusionDataType.BIGINT, DataFusionDataType.NULL)))));
        case BITWISE_AND:
            return new DataFusionBaseExpr("&", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE))),
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE)))));
        case BITWISE_OR:
            return new DataFusionBaseExpr("|", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE))),
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE)))));
        case BITWISE_XOR:
            return new DataFusionBaseExpr("^", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE))),
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE)))));
        case BITWISE_SHIFT_RIGHT:
            return new DataFusionBaseExpr(">>", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE))),
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT)))));
        case BITWISE_SHIFT_LEFT:
            return new DataFusionBaseExpr("<<", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE))),
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT)))));
        case NOT:
            return new DataFusionBaseExpr("NOT", 1, DataFusionBaseExprCategory.UNARY_PREFIX,
                    Arrays.asList(DataFusionDataType.BOOLEAN),
                    Arrays.asList(new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BOOLEAN)))));
        case PLUS: // unary prefix '+'
            return new DataFusionBaseExpr("+", 1, DataFusionBaseExprCategory.UNARY_PREFIX,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                    Arrays.asList(new ArgumentType.Fixed(
                            new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE)))));
        case MINUS: // unary prefix '-'
            return new DataFusionBaseExpr("-", 1, DataFusionBaseExprCategory.UNARY_PREFIX,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                    Arrays.asList(new ArgumentType.Fixed(
                            new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE)))));
        case MULTIPLICATION:
            return new DataFusionBaseExpr("*", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE))),
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE)))));
        case DIVISION:
            return new DataFusionBaseExpr("/", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE))),
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE)))));
        case MODULO:
            return new DataFusionBaseExpr("%", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE))),
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE)))));
        case EQUAL:
            return new DataFusionBaseExpr("=", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BOOLEAN),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING,
                                    DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE, DataFusionDataType.BOOLEAN))),
                            new ArgumentType.SameAsFirstArgType()));
        case EQUAL2:
            return new DataFusionBaseExpr("==", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BOOLEAN),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING,
                                    DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE, DataFusionDataType.BOOLEAN))),
                            new ArgumentType.SameAsFirstArgType()));
        case NOT_EQUAL:
            return new DataFusionBaseExpr("!=", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BOOLEAN),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING,
                                    DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE, DataFusionDataType.BOOLEAN))),
                            new ArgumentType.SameAsFirstArgType()));
        case LESS_THAN:
            return new DataFusionBaseExpr("<", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BOOLEAN),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING,
                                    DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE, DataFusionDataType.BOOLEAN))),
                            new ArgumentType.SameAsFirstArgType()));
        case LESS_THAN_OR_EQUAL_TO:
            return new DataFusionBaseExpr("<=", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BOOLEAN),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING,
                                    DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE, DataFusionDataType.BOOLEAN))),
                            new ArgumentType.SameAsFirstArgType()));
        case GREATER_THAN:
            return new DataFusionBaseExpr(">", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BOOLEAN),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING,
                                    DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE, DataFusionDataType.BOOLEAN))),
                            new ArgumentType.SameAsFirstArgType()));
        case GREATER_THAN_OR_EQUAL_TO:
            return new DataFusionBaseExpr(">=", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BOOLEAN),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING,
                                    DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE, DataFusionDataType.BOOLEAN))),
                            new ArgumentType.SameAsFirstArgType()));
        case IS_DISTINCT_FROM:
            return new DataFusionBaseExpr("IS DISTINCT FROM", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BOOLEAN),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING,
                                    DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE, DataFusionDataType.BOOLEAN))),
                            new ArgumentType.SameAsFirstArgType()));
        case IS_NOT_DISTINCT_FROM:
            return new DataFusionBaseExpr("IS NOT DISTINCT FROM", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BOOLEAN),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING,
                                    DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE, DataFusionDataType.BOOLEAN))),
                            new ArgumentType.SameAsFirstArgType()));
        // String related operators
        case LIKE:
            return DataFusionBaseExpr.createCommonStringOperatorTwoArgs(" LIKE ");
        case NOT_LIKE:
            return DataFusionBaseExpr.createCommonStringOperatorTwoArgs(" NOT LIKE ");
        case ILIKE:
            return DataFusionBaseExpr.createCommonStringOperatorTwoArgs(" ILIKE ");
        case NOT_ILIKE:
            return DataFusionBaseExpr.createCommonStringOperatorTwoArgs(" NOT ILIKE ");
        case REGEX_MATCH:
            return DataFusionBaseExpr.createCommonStringOperatorTwoArgs("~");
        case REGEX_CASE_INSENSITIVE_MATCH:
            return DataFusionBaseExpr.createCommonStringOperatorTwoArgs("~*");
        case NOT_REGEX_MATCH:
            return DataFusionBaseExpr.createCommonStringOperatorTwoArgs("!~");
        case NOT_REGEX_CASE_INSENSITIVE_MATCH:
            return DataFusionBaseExpr.createCommonStringOperatorTwoArgs("!~*");
        case LIKE_MATCH:
            return DataFusionBaseExpr.createCommonStringOperatorTwoArgs("~~");
        case CASE_INSENSITIVE_LIKE_MATCH:
            return DataFusionBaseExpr.createCommonStringOperatorTwoArgs("~~*");
        case NOT_LIKE_MATCH:
            return DataFusionBaseExpr.createCommonStringOperatorTwoArgs("!~~");
        case NOT_CASE_INSENSITIVE_LIKE_MATCH:
            return DataFusionBaseExpr.createCommonStringOperatorTwoArgs("!~~*");
        case STRING_CONCATENATION:
            return DataFusionBaseExpr.createCommonStringOperatorTwoArgs("||");
        // Logical Operators
        case AND:
            return new DataFusionBaseExpr("AND", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BOOLEAN),
                    Arrays.asList(new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BOOLEAN))), // arg1
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BOOLEAN))) // arg2
                    ));
        case OR:
            return new DataFusionBaseExpr("OR", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BOOLEAN),
                    Arrays.asList(new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BOOLEAN))), // arg1
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BOOLEAN))) // arg2
                    ));
        case ADD: // binary arithmetic operator '+'
            return new DataFusionBaseExpr("+", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BIGINT),
                    Arrays.asList(new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT))), // arg1
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT))) // arg2
                    ));
        case SUB: // binary arithmetic operator '-'
            return new DataFusionBaseExpr("-", 2, DataFusionBaseExprCategory.BINARY,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE))), // arg1
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE))) // arg2
                    ));
        // Scalar Functions
        case FUNC_ABS:
            return DataFusionBaseExpr.createCommonNumericFuncSingleArg("ABS");
        case FUNC_ACOS:
            return DataFusionBaseExpr.createCommonNumericFuncSingleArg("ACOS");
        case FUNC_ACOSH:
            return DataFusionBaseExpr.createCommonNumericFuncSingleArg("ACOSH");
        case FUNC_ASIN:
            return DataFusionBaseExpr.createCommonNumericFuncSingleArg("ASIN");
        case FUNC_ASINH:
            return DataFusionBaseExpr.createCommonNumericFuncSingleArg("ASINH");
        case FUNC_ATAN:
            return DataFusionBaseExpr.createCommonNumericFuncSingleArg("ATAN");
        case FUNC_ATANH:
            return DataFusionBaseExpr.createCommonNumericFuncSingleArg("ATANH");
        case FUNC_ATAN2:
            return DataFusionBaseExpr.createCommonNumericFuncTwoArgs("ATAN2");
        case FUNC_CBRT:
            return DataFusionBaseExpr.createCommonNumericFuncSingleArg("CBRT");
        case FUNC_CEIL:
            return DataFusionBaseExpr.createCommonNumericFuncSingleArg("CEIL");
        case FUNC_COS:
            return DataFusionBaseExpr.createCommonNumericFuncSingleArg("COS");
        case FUNC_COSH:
            return DataFusionBaseExpr.createCommonNumericFuncSingleArg("COSH");
        case FUNC_DEGREES:
            return DataFusionBaseExpr.createCommonNumericFuncSingleArg("DEGREES");
        case FUNC_EXP:
            return DataFusionBaseExpr.createCommonNumericFuncSingleArg("EXP");
        case FUNC_FACTORIAL:
            return DataFusionBaseExpr.createCommonNumericFuncSingleArg("FACTORIAL");
        case FUNC_FLOOR:
            return DataFusionBaseExpr.createCommonNumericFuncSingleArg("FLOOR");
        case FUNC_GCD:
            return new DataFusionBaseExpr("GCD", 2, DataFusionBaseExprCategory.FUNC,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE))),
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE)))));
        case FUNC_ISNAN:
            return DataFusionBaseExpr.createCommonNumericFuncSingleArg("ISNAN");
        case FUNC_ISZERO:
            return DataFusionBaseExpr.createCommonNumericFuncSingleArg("ISZERO");
        case FUNC_LCM:
            return DataFusionBaseExpr.createCommonNumericFuncTwoArgs("LCM");
        case FUNC_LN:
            return DataFusionBaseExpr.createCommonNumericFuncSingleArg("LN");
        case FUNC_LOG:
            return DataFusionBaseExpr.createCommonNumericFuncSingleArg("LOG");
        case FUNC_LOG_WITH_BASE:
            return DataFusionBaseExpr.createCommonNumericFuncTwoArgs("LOG");
        case FUNC_LOG10:
            return DataFusionBaseExpr.createCommonNumericFuncSingleArg("LOG10");
        case FUNC_LOG2:
            return DataFusionBaseExpr.createCommonNumericFuncSingleArg("LOG2");
        case FUNC_NANVL:
            return DataFusionBaseExpr.createCommonNumericFuncTwoArgs("NANVL");
        case FUNC_PI:
            return new DataFusionBaseExpr("PI", 0, DataFusionBaseExprCategory.FUNC,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE), Arrays.asList());
        case FUNC_POW:
            return DataFusionBaseExpr.createCommonNumericFuncSingleArg("POW");
        case FUNC_POWER:
            return DataFusionBaseExpr.createCommonNumericFuncSingleArg("POWER");
        case FUNC_RADIANS:
            return DataFusionBaseExpr.createCommonNumericFuncSingleArg("RADIANS");
        case FUNC_ROUND:
            return DataFusionBaseExpr.createCommonNumericFuncSingleArg("ROUND");
        case FUNC_ROUND_WITH_DECIMAL:
            return new DataFusionBaseExpr("ROUND", 2, DataFusionBaseExprCategory.FUNC,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE))),
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT)))));
        case FUNC_SIGNUM:
            return DataFusionBaseExpr.createCommonNumericFuncSingleArg("SIGNUM");
        case FUNC_SIN:
            return DataFusionBaseExpr.createCommonNumericFuncSingleArg("SIN");
        case FUNC_SINH:
            return DataFusionBaseExpr.createCommonNumericFuncSingleArg("SINH");
        case FUNC_SQRT:
            return DataFusionBaseExpr.createCommonNumericFuncSingleArg("SQRT");
        case FUNC_TAN:
            return DataFusionBaseExpr.createCommonNumericFuncSingleArg("TAN");
        case FUNC_TANH:
            return DataFusionBaseExpr.createCommonNumericFuncSingleArg("TANH");
        case FUNC_TRUNC:
            return DataFusionBaseExpr.createCommonNumericFuncSingleArg("TRUNC");
        case FUNC_TRUNC_WITH_DECIMAL:
            return new DataFusionBaseExpr("TRUNC", 2, DataFusionBaseExprCategory.FUNC,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(
                                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE))),
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT)))));
        case FUNC_COALESCE:
            return new DataFusionBaseExpr("COALESCE", -1, // overide by variadic
                    DataFusionBaseExprCategory.FUNC,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE, DataFusionDataType.BOOLEAN,
                            DataFusionDataType.STRING),
                    Arrays.asList(new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT,
                            DataFusionDataType.DOUBLE, DataFusionDataType.BOOLEAN, DataFusionDataType.STRING)))));
        case FUNC_NULLIF:
            return new DataFusionBaseExpr("NULLIF", 2, DataFusionBaseExprCategory.FUNC,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING,
                                    DataFusionDataType.BOOLEAN, DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE))),
                            new ArgumentType.Fixed(
                                    new ArrayList<>(Arrays.asList(DataFusionDataType.STRING, DataFusionDataType.BOOLEAN,
                                            DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE)))));
        case FUNC_NVL:
            return new DataFusionBaseExpr("NVL", 2, DataFusionBaseExprCategory.FUNC,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING,
                                    DataFusionDataType.BOOLEAN, DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE))),
                            new ArgumentType.Fixed(
                                    new ArrayList<>(Arrays.asList(DataFusionDataType.STRING, DataFusionDataType.BOOLEAN,
                                            DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE)))));
        case FUNC_NVL2:
            return new DataFusionBaseExpr("NVL2", 3, DataFusionBaseExprCategory.FUNC,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING,
                                    DataFusionDataType.BOOLEAN, DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE))),
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING,
                                    DataFusionDataType.BOOLEAN, DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE))),
                            new ArgumentType.Fixed(
                                    new ArrayList<>(Arrays.asList(DataFusionDataType.STRING, DataFusionDataType.BOOLEAN,
                                            DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE)))));
        case FUNC_IFNULL:
            return new DataFusionBaseExpr("IFNULL", 2, DataFusionBaseExprCategory.FUNC,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                    Arrays.asList(
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING,
                                    DataFusionDataType.BOOLEAN, DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE))),
                            new ArgumentType.Fixed(
                                    new ArrayList<>(Arrays.asList(DataFusionDataType.STRING, DataFusionDataType.BOOLEAN,
                                            DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE)))));

        case FUNC_ASCII:
            return DataFusionBaseExpr.createCommonStringFuncOneStringArg("ASCII",
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE));
        case FUNC_LENGTH:
            return DataFusionBaseExpr.createCommonStringFuncOneStringArg("LENGTH",
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE));
        case FUNC_CHAR_LENGTH:
            return DataFusionBaseExpr.createCommonStringFuncOneStringArg("CHAR_LENGTH",
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE));
        case FUNC_CHARACTER_LENGTH:
            return DataFusionBaseExpr.createCommonStringFuncOneStringArg("CHARACTER_LENGTH",
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE));
        case FUNC_BIT_LENGTH:
            return DataFusionBaseExpr.createCommonStringFuncOneStringArg("BIT_LENGTH",
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE));
        case FUNC_CHR:
            return new DataFusionBaseExpr("CHR", 1, DataFusionBaseExprCategory.FUNC,
                    Arrays.asList(DataFusionDataType.STRING),
                    Arrays.asList(new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT)))));
        case FUNC_INSTR:
            return DataFusionBaseExpr.createCommonStringFuncTwoStringArg("INSTR",
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE));
        case FUNC_STRPOS:
            return DataFusionBaseExpr.createCommonStringFuncTwoStringArg("STRPOS",
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE));
        case FUNC_LEVENSHTEIN:
            return DataFusionBaseExpr.createCommonStringFuncTwoStringArg("LEVENSHTEIN",
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE));
        case FUNC_FIND_IN_SET:
            return DataFusionBaseExpr.createCommonStringFuncTwoStringArg("FIND_IN_SET",
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE));
        case FUNC_INITCAP:
            return DataFusionBaseExpr.createCommonStringFuncOneStringArg("INITCAP",
                    Arrays.asList(DataFusionDataType.STRING));
        case FUNC_LOWER:
            return DataFusionBaseExpr.createCommonStringFuncOneStringArg("LOWER",
                    Arrays.asList(DataFusionDataType.STRING));
        case FUNC_UPPER:
            return DataFusionBaseExpr.createCommonStringFuncOneStringArg("UPPER",
                    Arrays.asList(DataFusionDataType.STRING));
        case FUNC_OCTET_LENGTH:
            return DataFusionBaseExpr.createCommonStringFuncOneStringArg("OCTET_LENGTH",
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE));
        case FUNC_BTRIM:
            return DataFusionBaseExpr.createCommonStringFuncOneStringArg("BTRIM",
                    Arrays.asList(DataFusionDataType.STRING));
        case FUNC_BTRIM2:
            return DataFusionBaseExpr.createCommonStringFuncTwoStringArg("BTRIM",
                    Arrays.asList(DataFusionDataType.STRING));
        case FUNC_TRIM:
            return DataFusionBaseExpr.createCommonStringFuncOneStringArg("TRIM",
                    Arrays.asList(DataFusionDataType.STRING));
        case FUNC_TRIM2:
            return DataFusionBaseExpr.createCommonStringFuncTwoStringArg("TRIM",
                    Arrays.asList(DataFusionDataType.STRING));
        case FUNC_LTRIM:
            return DataFusionBaseExpr.createCommonStringFuncOneStringArg("LTRIM",
                    Arrays.asList(DataFusionDataType.STRING));
        case FUNC_LTRIM2:
            return DataFusionBaseExpr.createCommonStringFuncTwoStringArg("LTRIM",
                    Arrays.asList(DataFusionDataType.STRING));
        case FUNC_RTRIM:
            return DataFusionBaseExpr.createCommonStringFuncOneStringArg("RTRIM",
                    Arrays.asList(DataFusionDataType.STRING));
        case FUNC_RTRIM2:
            return DataFusionBaseExpr.createCommonStringFuncTwoStringArg("RTRIM",
                    Arrays.asList(DataFusionDataType.STRING));
        case FUNC_LEFT:
            return new DataFusionBaseExpr("LEFT", 2, DataFusionBaseExprCategory.FUNC,
                    Arrays.asList(DataFusionDataType.STRING),
                    Arrays.asList(new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING))),
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT)))));
        case FUNC_RIGHT:
            return new DataFusionBaseExpr("RIGHT", 2, DataFusionBaseExprCategory.FUNC,
                    Arrays.asList(DataFusionDataType.STRING),
                    Arrays.asList(new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING))),
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT)))));
        case FUNC_CONCAT:
            return new DataFusionBaseExpr("CONCAT", -1, // overide by variadic
                    DataFusionBaseExprCategory.FUNC, Arrays.asList(DataFusionDataType.STRING),
                    Arrays.asList(new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT,
                            DataFusionDataType.DOUBLE, DataFusionDataType.BOOLEAN, DataFusionDataType.STRING)))));
        case FUNC_CONCAT_WS:
            return new DataFusionBaseExpr("CONCAT_WS", -1, // overide by variadic
                    DataFusionBaseExprCategory.FUNC, Arrays.asList(DataFusionDataType.STRING),
                    Arrays.asList(new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT,
                            DataFusionDataType.DOUBLE, DataFusionDataType.BOOLEAN, DataFusionDataType.STRING)))));
        // case FUNC_LPAD:
        // return new DataFusionBaseExpr("LPAD", 2, DataFusionBaseExprCategory.FUNC,
        // Arrays.asList(DataFusionDataType.STRING),
        // Arrays.asList(
        // new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING))),
        // new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT)))));
        // case FUNC_LPAD2:
        // return new DataFusionBaseExpr("LPAD", 3, DataFusionBaseExprCategory.FUNC,
        // Arrays.asList(DataFusionDataType.STRING),
        // Arrays.asList(
        // new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING))),
        // new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT))),
        // new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING)))));
        // case FUNC_RPAD:
        // return new DataFusionBaseExpr("RPAD", 2, DataFusionBaseExprCategory.FUNC,
        // Arrays.asList(DataFusionDataType.STRING),
        // Arrays.asList(
        // new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING))),
        // new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT)))));
        // case FUNC_RPAD2:
        // return new DataFusionBaseExpr("RPAD", 3, DataFusionBaseExprCategory.FUNC,
        // Arrays.asList(DataFusionDataType.STRING),
        // Arrays.asList(
        // new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING))),
        // new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT))),
        // new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING)))));
        // case FUNC_REPEAT:
        // return new DataFusionBaseExpr("REPEAT", 2, DataFusionBaseExprCategory.FUNC,
        // Arrays.asList(DataFusionDataType.STRING),
        // Arrays.asList(
        // new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING))),
        // new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT)))));
        case FUNC_REPLACE:
            return new DataFusionBaseExpr("REPLACE", 3, DataFusionBaseExprCategory.FUNC,
                    Arrays.asList(DataFusionDataType.STRING),
                    Arrays.asList(new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING))),
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING))),
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING)))));
        case FUNC_REVERSE:
            return DataFusionBaseExpr.createCommonStringFuncOneStringArg("REVERSE",
                    Arrays.asList(DataFusionDataType.STRING));
        case FUNC_SPLIT_PART:
            return new DataFusionBaseExpr("SPLIT_PART", 3, DataFusionBaseExprCategory.FUNC,
                    Arrays.asList(DataFusionDataType.STRING),
                    Arrays.asList(new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING))),
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING))),
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT)))));
        case FUNC_SUBSTR:
            return new DataFusionBaseExpr("SUBSTR", 2, DataFusionBaseExprCategory.FUNC,
                    Arrays.asList(DataFusionDataType.STRING),
                    Arrays.asList(new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING))),
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT)))));
        case FUNC_SUBSTR2:
            return new DataFusionBaseExpr("SUBSTR", 3, DataFusionBaseExprCategory.FUNC,
                    Arrays.asList(DataFusionDataType.STRING),
                    Arrays.asList(new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING))),
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT))),
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT)))));
        case FUNC_SUBSTRING:
            return new DataFusionBaseExpr("SUBSTRING", 2, DataFusionBaseExprCategory.FUNC,
                    Arrays.asList(DataFusionDataType.STRING),
                    Arrays.asList(new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING))),
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT)))));
        case FUNC_SUBSTRING2:
            return new DataFusionBaseExpr("SUBSTRING", 3, DataFusionBaseExprCategory.FUNC,
                    Arrays.asList(DataFusionDataType.STRING),
                    Arrays.asList(new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING))),
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT))),
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT)))));
        case FUNC_TRANSLATE:
            return new DataFusionBaseExpr("TRANSLATE", 3, DataFusionBaseExprCategory.FUNC,
                    Arrays.asList(DataFusionDataType.STRING),
                    Arrays.asList(new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING))),
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING))),
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING)))));
        case FUNC_TO_HEX:
            return new DataFusionBaseExpr("TO_HEX", 1, DataFusionBaseExprCategory.FUNC,
                    Arrays.asList(DataFusionDataType.STRING),
                    Arrays.asList(new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT)))));
        // case FUNC_UUID:
        // return new DataFusionBaseExpr("UUID", 0, DataFusionBaseExprCategory.FUNC,
        // Arrays.asList(DataFusionDataType.STRING),
        // Arrays.asList());
        case FUNC_SUBSTR_INDEX:
            return new DataFusionBaseExpr("SUBSTR_INDEX", 3, DataFusionBaseExprCategory.FUNC,
                    Arrays.asList(DataFusionDataType.STRING),
                    Arrays.asList(new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING))),
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING))),
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT)))));
        case FUNC_SUBSTRING_INDEX:
            return new DataFusionBaseExpr("SUBSTRING_INDEX", 3, DataFusionBaseExprCategory.FUNC,
                    Arrays.asList(DataFusionDataType.STRING),
                    Arrays.asList(new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING))),
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING))),
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BIGINT)))));
        case FUNC_ENDS_WITH:
            return DataFusionBaseExpr.createCommonStringFuncTwoStringArg("ENDS_WITH",
                    Arrays.asList(DataFusionDataType.BOOLEAN));
        case FUNC_STARTS_WITH:
            return DataFusionBaseExpr.createCommonStringFuncTwoStringArg("STARTS_WITH",
                    Arrays.asList(DataFusionDataType.BOOLEAN));
        case FUNC_REGEXP_LIKE:
            return DataFusionBaseExpr.createCommonStringFuncTwoStringArg("REGEXP_LIKE",
                    Arrays.asList(DataFusionDataType.BOOLEAN));
        case FUNC_REGEXP_LIKE2:
            return new DataFusionBaseExpr("REGEXP_LIKE", 3, DataFusionBaseExprCategory.FUNC,
                    Arrays.asList(DataFusionDataType.BOOLEAN),
                    Arrays.asList(new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING))),
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING))),
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING)))));
        case FUNC_REGEXP_MATCH:
            return DataFusionBaseExpr.createCommonStringFuncTwoStringArg("REGEXP_MATCH",
                    Arrays.asList(DataFusionDataType.STRING)); // TODO(datafusion)
        // return
        // type
        // change
        // to
        // List
        // after
        // List
        // is
        // supported
        case FUNC_REGEXP_MATCH2:
            return new DataFusionBaseExpr("REGEXP_MATCH", 3, DataFusionBaseExprCategory.FUNC,
                    Arrays.asList(DataFusionDataType.STRING),
                    Arrays.asList(new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING))),
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING))),
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING)))));
        case FUNC_REGEXP_REPLACE:
            return DataFusionBaseExpr.createCommonStringFuncTwoStringArg("REGEXP_REPLACE",
                    Arrays.asList(DataFusionDataType.STRING));
        case FUNC_REGEXP_REPLACE2:
            return new DataFusionBaseExpr("REGEXP_REPLACE", 3, DataFusionBaseExprCategory.FUNC,
                    Arrays.asList(DataFusionDataType.STRING),
                    Arrays.asList(new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING))),
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING))),
                            new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.STRING)))));
        case AGGR_MIN:
            return DataFusionBaseExpr.createCommonNumericAggrFuncSingleArg("MIN");
        case AGGR_MAX:
            return DataFusionBaseExpr.createCommonNumericAggrFuncSingleArg("MAX");
        case AGGR_AVG:
            return DataFusionBaseExpr.createCommonNumericAggrFuncSingleArg("AVG");
        case AGGR_SUM:
            return DataFusionBaseExpr.createCommonNumericAggrFuncSingleArg("SUM");
        case AGGR_COUNT:
            return new DataFusionBaseExpr("COUNT", -1, DataFusionBaseExprCategory.AGGREGATE,
                    Arrays.asList(DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE),
                    Arrays.asList(new ArgumentType.Fixed(new ArrayList<>(Arrays.asList(DataFusionDataType.BOOLEAN,
                            DataFusionDataType.BIGINT, DataFusionDataType.DOUBLE)))),
                    true);
        default:
            dfAssert(false, "Unreachable. Unimplemented branch for type " + type);
        }

        dfAssert(false, "Unreachable. Unimplemented branch for type " + type);
        return null;
    }

    // if input is Optional.empty(), return all possible `DataFusionBaseExpr`s
    // else, return all `DataFusionBaseExpr` which might be evaluated to arg's type
    public static List<DataFusionBaseExpr> getExprsWithReturnType(Optional<DataFusionDataType> dataTypeOptional) {
        List<DataFusionBaseExpr> allExpressions = Arrays.stream(DataFusionBaseExprType.values())
                .map(DataFusionBaseExprFactory::createExpr).collect(Collectors.toList());

        if (!dataTypeOptional.isPresent()) {
            return allExpressions; // If Optional is empty, return all expressions
        }

        DataFusionDataType filterType = dataTypeOptional.get();
        List<DataFusionBaseExpr> exprsWithReturnType = allExpressions.stream()
                .filter(expr -> expr.possibleReturnTypes.contains(filterType)).collect(Collectors.toList());

        if (Randomly.getBoolean()) {
            // Too many similar function, so test them less often
            return exprsWithReturnType;
        }

        return exprsWithReturnType.stream().filter(expr -> expr.exprType != DataFusionBaseExprCategory.FUNC)
                .collect(Collectors.toList());
    }

    public static DataFusionBaseExpr getRandomAggregateExpr() {
        List<DataFusionBaseExpr> allAggrExpressions = Arrays.stream(DataFusionBaseExprType.values())
                .map(DataFusionBaseExprFactory::createExpr)
                .filter(expr -> expr.exprType == DataFusionBaseExprCategory.AGGREGATE).collect(Collectors.toList());

        return Randomly.fromList(allAggrExpressions);
    }
}
