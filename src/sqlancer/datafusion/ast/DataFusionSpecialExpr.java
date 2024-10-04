package sqlancer.datafusion.ast;

import sqlancer.common.ast.newast.Node;

public class DataFusionSpecialExpr {
    public static class CastToStringView implements Node<DataFusionExpression> {
        public Node<DataFusionExpression> expr;

        public CastToStringView(Node<DataFusionExpression> expr) {
            this.expr = expr;
        }
    }
}
