package pgxaip

import (
	"fmt"
	"time"

	"go.einride.tech/aip/filtering"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// rewriteFilter transpiles an AIP-160 [filtering.Filter] into a Postgres
// WHERE fragment (no enclosing parentheses) and the bound arguments that
// match its placeholders. Placeholder numbering starts at startParam so
// callers can splice the fragment into a query that already has earlier
// bound values.
//
// columns is the AIP-path → DB-column allow-list. Lookup is fail-closed:
// any ident in the filter that is absent from columns causes an error.
//
// Returns ("", nil, nil) when the filter is empty (zero Filter or nil
// CheckedExpr); callers use that to decide whether to emit a WHERE clause.
func rewriteFilter(f filtering.Filter, columns map[string]string, startParam int) (string, []any, error) {
	if f.CheckedExpr == nil {
		return "", nil, nil
	}
	e := f.CheckedExpr.GetExpr()
	if e == nil {
		return "", nil, nil
	}
	t := &transpiler{columns: columns, startParam: startParam}
	sql, err := t.transpile(e)
	if err != nil {
		return "", nil, err
	}
	return sql, t.args, nil
}

type transpiler struct {
	args       []any
	columns    map[string]string
	startParam int
}

func (t *transpiler) placeholder(v any) string {
	t.args = append(t.args, v)
	return fmt.Sprintf("$%d", t.startParam+len(t.args)-1)
}

func (t *transpiler) transpile(e *exprpb.Expr) (string, error) {
	switch v := e.ExprKind.(type) {
	case *exprpb.Expr_ConstExpr:
		return t.transpileConst(v.ConstExpr)
	case *exprpb.Expr_IdentExpr, *exprpb.Expr_SelectExpr:
		return t.transpileIdent(e)
	case *exprpb.Expr_CallExpr:
		return t.transpileCall(v.CallExpr)
	default:
		return "", fmt.Errorf("unsupported expression kind %T", v)
	}
}

func (t *transpiler) transpileIdent(e *exprpb.Expr) (string, error) {
	path, ok := identPath(e)
	if !ok {
		return "", fmt.Errorf("unsupported identifier expression %T", e.ExprKind)
	}
	col, ok := t.columns[path]
	if !ok {
		return "", fmt.Errorf("unknown field %q", path)
	}
	return sanitizePath(col), nil
}

// identPath reconstructs a dotted AIP path (e.g. "address.city") from a
// chain of Ident/Select expressions. Returns false if the expression is
// not a pure identifier chain.
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
	switch v := c.ConstantKind.(type) {
	case *exprpb.Constant_StringValue:
		return t.placeholder(v.StringValue), nil
	case *exprpb.Constant_Int64Value:
		return t.placeholder(v.Int64Value), nil
	case *exprpb.Constant_Uint64Value:
		return t.placeholder(v.Uint64Value), nil
	case *exprpb.Constant_DoubleValue:
		return t.placeholder(v.DoubleValue), nil
	case *exprpb.Constant_BoolValue:
		return t.placeholder(v.BoolValue), nil
	default:
		return "", fmt.Errorf("unsupported constant kind %T", v)
	}
}

func (t *transpiler) transpileCall(call *exprpb.Expr_Call) (string, error) {
	switch call.Function {
	case filtering.FunctionEquals, filtering.FunctionNotEquals,
		filtering.FunctionLessThan, filtering.FunctionLessEquals,
		filtering.FunctionGreaterThan, filtering.FunctionGreaterEquals:
		return t.transpileComparison(call)

	case filtering.FunctionAnd, filtering.FunctionFuzzyAnd:
		return t.transpileBinary(call, "AND")

	case filtering.FunctionOr:
		return t.transpileBinary(call, "OR")

	case filtering.FunctionNot:
		if len(call.Args) != 1 {
			return "", fmt.Errorf("NOT expects 1 argument, got %d", len(call.Args))
		}
		operand, err := t.transpile(call.Args[0])
		if err != nil {
			return "", err
		}
		return "(NOT " + operand + ")", nil

	case filtering.FunctionHas:
		if len(call.Args) != 2 {
			return "", fmt.Errorf(": expects 2 arguments, got %d", len(call.Args))
		}
		lhs, err := t.transpile(call.Args[0])
		if err != nil {
			return "", err
		}
		rhs, err := t.transpile(call.Args[1])
		if err != nil {
			return "", err
		}
		return lhs + " ILIKE '%' || " + rhs + " || '%'", nil

	case filtering.FunctionTimestamp:
		return t.transpileTimeFunc(call, parseTimestamp)

	case filtering.FunctionDuration:
		return t.transpileTimeFunc(call, parseDuration)

	// Unary minus is emitted by the parser as the "-_" function.
	case "-_":
		return t.transpileUnaryMinus(call)

	default:
		return "", fmt.Errorf("unsupported function %q", call.Function)
	}
}

// transpileComparison handles =, !=, <, <=, >, >=. Each side may be an
// identifier (resolved through the column map) or a literal (bound as a
// placeholder). Column-to-column comparisons are supported.
func (t *transpiler) transpileComparison(call *exprpb.Expr_Call) (string, error) {
	if len(call.Args) != 2 {
		return "", fmt.Errorf("%s expects 2 arguments, got %d", call.Function, len(call.Args))
	}
	lhs, err := t.transpile(call.Args[0])
	if err != nil {
		return "", err
	}
	rhs, err := t.transpile(call.Args[1])
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s %s %s", lhs, call.Function, rhs), nil
}

func (t *transpiler) transpileBinary(call *exprpb.Expr_Call, op string) (string, error) {
	if len(call.Args) != 2 {
		return "", fmt.Errorf("%s expects 2 arguments, got %d", op, len(call.Args))
	}
	lhs, err := t.transpile(call.Args[0])
	if err != nil {
		return "", err
	}
	rhs, err := t.transpile(call.Args[1])
	if err != nil {
		return "", err
	}
	return "(" + lhs + " " + op + " " + rhs + ")", nil
}

// transpileTimeFunc binds timestamp("...") / duration("...") literals as
// concrete time.Time / time.Duration values so drivers can marshal them
// natively. The CEL argument must be a string constant.
func (t *transpiler) transpileTimeFunc(
	call *exprpb.Expr_Call,
	parse func(string) (any, error),
) (string, error) {
	if len(call.Args) != 1 {
		return "", fmt.Errorf("%s expects 1 argument, got %d", call.Function, len(call.Args))
	}
	s, ok := stringLiteral(call.Args[0])
	if !ok {
		return "", fmt.Errorf("%s argument must be a string literal", call.Function)
	}
	v, err := parse(s)
	if err != nil {
		return "", fmt.Errorf("%s: %w", call.Function, err)
	}
	return t.placeholder(v), nil
}

// transpileUnaryMinus folds -<literal> into a single signed placeholder.
// Anything else (e.g. -column) is rejected; CEL's type checker normally
// blocks those before reaching us.
func (t *transpiler) transpileUnaryMinus(call *exprpb.Expr_Call) (string, error) {
	if len(call.Args) != 1 {
		return "", fmt.Errorf("unary minus expects 1 argument, got %d", len(call.Args))
	}
	c, ok := call.Args[0].ExprKind.(*exprpb.Expr_ConstExpr)
	if !ok {
		return "", fmt.Errorf("unary minus requires a numeric literal")
	}
	switch v := c.ConstExpr.ConstantKind.(type) {
	case *exprpb.Constant_Int64Value:
		return t.placeholder(-v.Int64Value), nil
	case *exprpb.Constant_DoubleValue:
		return t.placeholder(-v.DoubleValue), nil
	default:
		return "", fmt.Errorf("unary minus requires a numeric literal")
	}
}

func stringLiteral(e *exprpb.Expr) (string, bool) {
	c, ok := e.ExprKind.(*exprpb.Expr_ConstExpr)
	if !ok {
		return "", false
	}
	s, ok := c.ConstExpr.ConstantKind.(*exprpb.Constant_StringValue)
	if !ok {
		return "", false
	}
	return s.StringValue, true
}

func parseTimestamp(s string) (any, error) {
	return time.Parse(time.RFC3339, s)
}

func parseDuration(s string) (any, error) {
	return time.ParseDuration(s)
}
