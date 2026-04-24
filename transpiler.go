package pgxquery

import (
	"fmt"

	"go.einride.tech/aip/filtering"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

type transpiler struct {
	args    []any
	columns map[string]string
}

func (t *transpiler) transpile(e *exprpb.Expr) (string, error) {
	switch v := e.ExprKind.(type) {
	case *exprpb.Expr_ConstExpr:
		return t.transpileConst(v.ConstExpr)
	case *exprpb.Expr_IdentExpr, *exprpb.Expr_SelectExpr:
		path, ok := identPath(e)
		if !ok {
			return "", fmt.Errorf("unsupported identifier expression %T", v)
		}
		col, ok := t.columns[path]
		if !ok {
			return "", fmt.Errorf("unknown field %q", path)
		}
		return sanitizePath(col), nil
	case *exprpb.Expr_CallExpr:
		return t.transpileCall(v.CallExpr)
	default:
		return "", fmt.Errorf("unsupported expression kind %T", v)
	}
}

// identPath reconstructs a dotted AIP path (e.g. `address.city`) from a chain
// of Ident/Select expressions. Returns false if the expression is not a pure
// identifier chain.
func identPath(e *exprpb.Expr) (string, bool) {
	switch v := e.ExprKind.(type) {
	case *exprpb.Expr_IdentExpr:
		return v.IdentExpr.Name, true
	case *exprpb.Expr_SelectExpr:
		op, ok := identPath(v.SelectExpr.Operand)
		if !ok {
			return "", false
		}
		return op + "." + v.SelectExpr.Field, true
	default:
		return "", false
	}
}

func (t *transpiler) transpileConst(c *exprpb.Constant) (string, error) {
	var val any
	switch v := c.ConstantKind.(type) {
	case *exprpb.Constant_StringValue:
		val = v.StringValue
	case *exprpb.Constant_Int64Value:
		val = v.Int64Value
	case *exprpb.Constant_DoubleValue:
		val = v.DoubleValue
	case *exprpb.Constant_BoolValue:
		val = v.BoolValue
	case *exprpb.Constant_Uint64Value:
		val = v.Uint64Value
	default:
		return "", fmt.Errorf("unsupported constant kind %T", v)
	}
	t.args = append(t.args, val)
	return fmt.Sprintf("$%d", len(t.args)), nil
}

func (t *transpiler) transpileCall(call *exprpb.Expr_Call) (string, error) {
	switch call.Function {
	case filtering.FunctionEquals, filtering.FunctionNotEquals,
		filtering.FunctionLessThan, filtering.FunctionLessEquals,
		filtering.FunctionGreaterThan, filtering.FunctionGreaterEquals:
		lhs, err := t.transpile(call.Args[0])
		if err != nil {
			return "", err
		}
		rhs, err := t.transpile(call.Args[1])
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s %s %s", lhs, call.Function, rhs), nil

	case filtering.FunctionAnd, filtering.FunctionFuzzyAnd:
		lhs, err := t.transpile(call.Args[0])
		if err != nil {
			return "", err
		}
		rhs, err := t.transpile(call.Args[1])
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("(%s AND %s)", lhs, rhs), nil

	case filtering.FunctionOr:
		lhs, err := t.transpile(call.Args[0])
		if err != nil {
			return "", err
		}
		rhs, err := t.transpile(call.Args[1])
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("(%s OR %s)", lhs, rhs), nil

	case filtering.FunctionNot:
		operand, err := t.transpile(call.Args[0])
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("(NOT %s)", operand), nil

	case filtering.FunctionHas:
		lhs, err := t.transpile(call.Args[0])
		if err != nil {
			return "", err
		}
		rhs, err := t.transpile(call.Args[1])
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s ILIKE '%%' || %s || '%%'", lhs, rhs), nil

	default:
		return "", fmt.Errorf("unsupported function %q", call.Function)
	}
}
