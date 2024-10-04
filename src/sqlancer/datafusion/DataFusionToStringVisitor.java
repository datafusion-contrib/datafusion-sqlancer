package sqlancer.datafusion;

import static sqlancer.datafusion.DataFusionUtil.dfAssert;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.NewToStringVisitor;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.datafusion.ast.DataFusionConstant;
import sqlancer.datafusion.ast.DataFusionExpression;
import sqlancer.datafusion.ast.DataFusionSelect;
import sqlancer.datafusion.ast.DataFusionSelect.DataFusionFrom;
import sqlancer.datafusion.DataFusionSchema.DataFusionTable;
import sqlancer.datafusion.ast.DataFusionWindowExpr;
import sqlancer.datafusion.ast.DataFusionSpecialExpr.CastToStringView;
import sqlancer.datafusion.ast.DataFusionSelect.DataFusionAlias;

public class DataFusionToStringVisitor extends NewToStringVisitor<DataFusionExpression> {

    public static String asString(Node<DataFusionExpression> expr) {
        DataFusionToStringVisitor visitor = new DataFusionToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

    public static String asString(List<Node<DataFusionExpression>> exprs) {
        DataFusionToStringVisitor visitor = new DataFusionToStringVisitor();
        visitor.visit(exprs);
        return visitor.get();
    }

    @Override
    public void visitSpecific(Node<DataFusionExpression> expr) {
        if (expr instanceof DataFusionConstant) {
            visit((DataFusionConstant) expr);
        } else if (expr instanceof DataFusionSelect) {
            visit((DataFusionSelect) expr);
        } else if (expr instanceof DataFusionFrom) {
            visit((DataFusionFrom) expr);
        } else if (expr instanceof DataFusionWindowExpr) {
            visit((DataFusionWindowExpr) expr);
        } else if (expr instanceof CastToStringView) {
            visit((CastToStringView) expr);
        } else if (expr instanceof DataFusionAlias) {
            visit((DataFusionAlias) expr);
        } else {
            throw new AssertionError(expr.getClass());
        }
    }

    private void visit(DataFusionFrom from) {
        sb.append(" FROM ");

        dfAssert(from.joinTypeList.size() == from.joinConditionList.size(), "Validate from");

        /* e.g. from t1, t2, t3 */
        if (from.joinConditionList.isEmpty()) {
            visit(from.tableList);
            return;
        }

        dfAssert(from.joinConditionList.size() == from.tableList.size() - 1, "Validate from");
        /* e.g. from t1 join t2 on t1.v1=t2.v1 */
        visit(from.tableList.get(0));
        for (int i = 0; i < from.joinConditionList.size(); i++) {
            switch (from.joinTypeList.get(i)) {
                case INNER:
                    sb.append(Randomly.fromOptions(" JOIN ", " INNER JOIN "));
                    break;
                case LEFT:
                    sb.append(Randomly.fromOptions(" LEFT JOIN ", " LEFT OUTER JOIN "));
                    break;
                case RIGHT:
                    sb.append(Randomly.fromOptions(" RIGHT JOIN ", " RIGHT OUTER JOIN "));
                    break;
                case FULL:
                    sb.append(Randomly.fromOptions(" FULL JOIN ", " FULL OUTER JOIN "));
                    break;
                case CROSS:
                    sb.append(" CROSS JOIN ");
                    break;
                case NATURAL:
                    sb.append(" NATURAL JOIN ");
                    break;
                default:
                    dfAssert(false, "Unreachable");
            }

            visit(from.tableList.get(i + 1)); // ti

            /* ON ... */
            Node<DataFusionExpression> cond = from.joinConditionList.get(i);
            if (cond != null) {
                sb.append(" ON ");
                visit(cond);
            }
        }

    }

    private void visit(DataFusionConstant constant) {
        sb.append(constant.toString());
    }

    private void visit(DataFusionSelect select) {
        sb.append("SELECT ");
        if (select.all && !select.distinct) {
            sb.append("ALL ");
        }
        if (select.distinct) {
            sb.append("DISTINCT ");
        }
        if (select.fetchColumnsString.isPresent()) {
            sb.append(select.fetchColumnsString.get());
        } else {
            visit(select.getFetchColumns());
        }

        visit(select.from);
        if (select.getWhereClause() != null) {
            sb.append(" WHERE ");
            visit(select.getWhereClause());
        }
        if (!select.getGroupByExpressions().isEmpty()) {
            sb.append(" GROUP BY ");
            visit(select.getGroupByExpressions());
        }
        if (select.getHavingClause() != null) {
            sb.append(" HAVING ");
            visit(select.getHavingClause());
        }
        if (!select.getOrderByClauses().isEmpty()) {
            sb.append(" ORDER BY ");
            visit(select.getOrderByClauses());
        }
        if (select.getLimitClause() != null) {
            sb.append(" LIMIT ");
            visit(select.getLimitClause());
        }
        if (select.getOffsetClause() != null) {
            sb.append(" OFFSET ");
            visit(select.getOffsetClause());
        }
    }

    private void visit(DataFusionWindowExpr window) {
        // Window function
        visit(window.windowFunc);

        // Over clause
        // -----------
        sb.append(" OVER (");

        if (!window.partitionByList.isEmpty()) {
            sb.append("PARTITION BY ");
            visit(window.partitionByList);
        }

        if (!window.orderByList.isEmpty()) {
            sb.append(" ORDER BY ");
            visit(window.orderByList);
        }

        if (window.frameClause.isPresent()) {
            sb.append(" ");
            sb.append(window.frameClause.get());
        }

        sb.append(")");
    }

    private void visit(CastToStringView castToStringView) {
        sb.append("ARROW_CAST(");
        visit(castToStringView.expr);
        sb.append(", 'Utf8')");
    }

    private void visit(DataFusionAlias alias) {
        if (alias.table instanceof TableReferenceNode) {
            DataFusionTable t = null;
            if (alias.table instanceof TableReferenceNode) {
                t = ((TableReferenceNode<DataFusionExpression, DataFusionTable>) alias.table).getTable();
            } else {
                dfAssert(false, "Unreachable");
            }

            String baseName = t.getNotAliasedName();
            sb.append(baseName);

            dfAssert(t.alias.isPresent(), "Alias should be present");
            sb.append(" AS ");
            sb.append(t.alias.get());
        } else {
            dfAssert(false, "Unreachable");
        }
    }

}
