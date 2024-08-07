package sqlancer.common.ast.newast;

import java.util.Optional;

import sqlancer.Randomly;

public class NewOrderingTerm<T> implements Node<T> {

    private final Node<T> expr;
    private final Ordering ordering;
    private final Optional<OrderingNulls> orderingNullsOptional;

    public enum Ordering {
        ASC, DESC;

        public static Ordering getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public enum OrderingNulls {
        NULLS_FIRST, NULLS_LAST;

        public static OrderingNulls getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String toString() {
            switch (this) {
            case NULLS_FIRST:
                return "NULLS FIRST";
            case NULLS_LAST:
                return "NULLS LAST";
            default:
                throw new AssertionError("Unreachable");
            }
        }
    }

    public NewOrderingTerm(Node<T> expr, Ordering ordering) {
        this.expr = expr;
        this.ordering = ordering;
        this.orderingNullsOptional = Optional.empty();
    }

    public NewOrderingTerm(Node<T> expr, Ordering ordering, OrderingNulls orderingNulls) {
        this.expr = expr;
        this.ordering = ordering;
        this.orderingNullsOptional = Optional.of(orderingNulls);
    }

    public Node<T> getExpr() {
        return expr;
    }

    public Ordering getOrdering() {
        return ordering;
    }

    public Optional<OrderingNulls> getOrderingNullsOptional() {
        return orderingNullsOptional;
    }
}
