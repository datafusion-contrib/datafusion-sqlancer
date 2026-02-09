package sqlancer.datafusion.ast;

import sqlancer.common.ast.newast.Node;

public class DataFusionConstant implements Node<DataFusionExpression> {

    private DataFusionConstant() {
    }

    public static Node<DataFusionExpression> createIntConstant(long val) {
        return new DataFusionIntConstant(val);
    }

    public static Node<DataFusionExpression> createNullConstant() {
        return new DataFusionNullConstant();
    }

    public static Node<DataFusionExpression> createDateConstant(long daysFromEpoch) {
        return new DataFusionDateConstant(daysFromEpoch);
    }

    public static Node<DataFusionExpression> createTimestampConstant(long secondsSinceEpoch) {
        return new DataFusionTimestampConstant(secondsSinceEpoch);
    }

    public static Node<DataFusionExpression> createTimeConstant(long secondsInDay) {
        return new DataFusionTimeConstant(secondsInDay);
    }

    public static class DataFusionNullConstant extends DataFusionConstant {

        @Override
        public String toString() {
            return "NULL";
        }

    }

    public static class DataFusionIntConstant extends DataFusionConstant {

        private final long value;

        public DataFusionIntConstant(long value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        public long getValue() {
            return value;
        }

    }

    public static class DataFusionDoubleConstant extends DataFusionConstant {

        private final String valueStr;

        public DataFusionDoubleConstant(double value) {
            if (value == Double.POSITIVE_INFINITY) {
                valueStr = "'+Inf'::Double";
            } else if (value == Double.NEGATIVE_INFINITY) {
                valueStr = "'-Inf'::Double";
            } else if (Double.isNaN(value)) {
                valueStr = "'NaN'::Double";
            } else if (Double.compare(value, -0.0) == 0) {
                valueStr = "-0.0";
            } else {
                valueStr = String.valueOf(value);
            }
        }

        // Make it more convenient to construct special value like -0, NaN, etc.
        public DataFusionDoubleConstant(String valueStr) {
            this.valueStr = valueStr;
        }

        @Override
        public String toString() {
            return valueStr;
        }

    }

    public static class DataFusionBooleanConstant extends DataFusionConstant {

        private final boolean value;

        public DataFusionBooleanConstant(boolean value) {
            this.value = value;
        }

        public boolean getValue() {
            return value;
        }

        @Override
        public String toString() {
            if (value) {
                return "true";
            } else {
                return "false";
            }
        }

    }

    public static class DataFusionStringConstant extends DataFusionConstant {
        private final String value;

        public static String cleanString(String input) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < input.length(); i++) {
                char c = input.charAt(i);
                // Check if the character is a high surrogate
                if (Character.isHighSurrogate(c)) {
                    if (i + 1 < input.length() && Character.isLowSurrogate(input.charAt(i + 1))) {
                        // It's a valid surrogate pair, add both to the string
                        sb.append(c);
                        sb.append(input.charAt(i + 1));
                        i++; // Skip the next character as it's part of the surrogate pair
                    }
                } else if (!Character.isLowSurrogate(c) && !Character.isSurrogate(c)) {
                    // Add only if it's not a low surrogate or any standalone surrogate
                    sb.append(c);
                }
            }
            return sb.toString();
        }

        public DataFusionStringConstant(String value) {
            // cleanup invalid Utf8
            this.value = cleanString(value.replace("'", "''"));
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "'" + value + "'";
        }

    }

    public static class DataFusionDateConstant extends DataFusionConstant {
        private final String value;

        public DataFusionDateConstant(long daysFromEpoch) {
            // Convert days from epoch to date string
            // Epoch is 1970-01-01, so we add/subtract days
            long totalDays = daysFromEpoch;

            // Simple date calculation (not accounting for all edge cases, but good enough for fuzzing)
            // Start from 1970-01-01
            int year = 1970;
            int month = 1;
            int day = 1;

            // Approximate calculation for fuzzing purposes
            if (totalDays >= 0) {
                year += (int) (totalDays / 365);
                totalDays = totalDays % 365;
                month = (int) (totalDays / 30) + 1;
                day = (int) (totalDays % 30) + 1;
            } else {
                totalDays = -totalDays;
                year -= (int) (totalDays / 365);
                totalDays = totalDays % 365;
                month = (int) (totalDays / 30) + 1;
                day = (int) (totalDays % 30) + 1;
            }

            // Keep values in valid ranges
            if (year < 1) {
                year = 1;
            }
            if (year > 9999) {
                year = 9999;
            }
            if (month < 1) {
                month = 1;
            }
            if (month > 12) {
                month = 12;
            }
            if (day < 1) {
                day = 1;
            }
            if (day > 28) {
                day = 28; // Safe for all months
            }

            this.value = String.format("%04d-%02d-%02d", year, month, day);
        }

        @Override
        public String toString() {
            return "DATE '" + value + "'";
        }
    }

    public static class DataFusionTimestampConstant extends DataFusionConstant {
        private final String value;

        public DataFusionTimestampConstant(long secondsSinceEpoch) {
            // Convert seconds to timestamp string YYYY-MM-DD HH:MM:SS
            // Simple approximation for fuzzing
            long totalSeconds = Math.abs(secondsSinceEpoch);
            long seconds = totalSeconds % 60;
            long totalMinutes = totalSeconds / 60;
            long minutes = totalMinutes % 60;
            long totalHours = totalMinutes / 60;
            long hours = totalHours % 24;
            long totalDays = totalHours / 24;

            // Calculate date from days (approximate)
            int year = 1970;
            int month = 1;
            int day = 1;

            if (secondsSinceEpoch >= 0) {
                year += (int) (totalDays / 365);
                totalDays = totalDays % 365;
                month = (int) (totalDays / 30) + 1;
                day = (int) (totalDays % 30) + 1;
            } else {
                year -= (int) (totalDays / 365);
                totalDays = totalDays % 365;
                month = (int) (totalDays / 30) + 1;
                day = (int) (totalDays % 30) + 1;
            }

            // Keep in valid ranges
            if (year < 1) {
                year = 1;
            }
            if (year > 9999) {
                year = 9999;
            }
            if (month < 1) {
                month = 1;
            }
            if (month > 12) {
                month = 12;
            }
            if (day < 1) {
                day = 1;
            }
            if (day > 28) {
                day = 28;
            }

            this.value = String.format("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hours, minutes, seconds);
        }

        @Override
        public String toString() {
            return "TIMESTAMP '" + value + "'";
        }
    }

    public static class DataFusionTimeConstant extends DataFusionConstant {
        private final String value;

        public DataFusionTimeConstant(long secondsInDay) {
            // Convert seconds to HH:MM:SS
            long totalSeconds = secondsInDay % 86400; // Ensure within a day
            if (totalSeconds < 0) {
                totalSeconds += 86400;
            }

            long hours = totalSeconds / 3600;
            long minutes = (totalSeconds % 3600) / 60;
            long seconds = totalSeconds % 60;

            this.value = String.format("%02d:%02d:%02d", hours, minutes, seconds);
        }

        @Override
        public String toString() {
            return "TIME '" + value + "'";
        }
    }
}
