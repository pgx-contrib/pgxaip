// Package pgxaip rewrites AIP-160 filter, AIP-132 order_by, and keyset
// cursor values into Postgres SQL fragments that the caller splices into
// a query by hand.
//
// The entry point is [Query]. Populate it from the request parsers (for
// example the ones emitted by protoc-gen-go-aip-query) and call
// [Query.Rewrite] to produce the WHERE predicate, the ORDER BY list, and
// the matching positional args. Pagination is offset- or cursor-based —
// driven by [pagination.PageToken.Cursor] when set — and the caller
// tacks the LIMIT/OFFSET pair onto the SQL themselves.
package pgxaip

import (
	"fmt"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/operators"
	"github.com/google/cel-go/common/overloads"
	"github.com/pgx-contrib/pgxcel"
	"go.einride.tech/aip/filtering"
	"go.einride.tech/aip/ordering"
	"go.einride.tech/aip/pagination"
)

// Query bundles the parsed AIP inputs for a List request together with
// the AIP-path → DB-column allow-list that the rewriter uses to resolve
// identifiers. Any path in Filter or OrderBy that is not in Columns
// causes [Query.Rewrite] to fail.
//
// OrderBy is the effective ordering — user-supplied fields plus any
// tiebreaker (the primary key, typically) the caller appends. When
// PageToken.Cursor is non-empty, its length must match len(OrderBy.Fields).
type Query struct {
	Filter    filtering.Filter
	OrderBy   ordering.OrderBy
	PageToken pagination.PageToken
	Columns   map[string]string
}

// Rewrite returns the SQL fragments and bound args derived from the
// query's filter, ordering, and cursor.
//
//   - where  is the WHERE predicate: the filter expression, the keyset
//     cursor predicate, or the two ANDed together. Empty string when
//     neither is present — the caller should omit the WHERE keyword.
//   - order  is the comma-separated ORDER BY list (no "ORDER BY" prefix).
//     Empty string when OrderBy has no fields.
//   - args   are the positional bind values, numbered $1..$N. Filter
//     literals come first, then cursor values. The caller appends its
//     own LIMIT/OFFSET values at $N+1 and onward.
//
// PageToken.Offset is not consulted; the caller feeds it into an OFFSET
// clause themselves.
func (q Query) Rewrite() (where, order string, args []any, err error) {
	filterSQL, filterArgs, err := rewriteFilter(q.Filter, q.Columns, 1)
	if err != nil {
		return "", "", nil, err
	}

	cursorSQL, cursorArgs, err := rewriteCursor(
		q.OrderBy.Fields,
		q.PageToken.Cursor,
		q.Columns,
		1+len(filterArgs),
	)
	if err != nil {
		return "", "", nil, err
	}

	orderSQL, err := rewriteOrderBy(q.OrderBy, q.Columns)
	if err != nil {
		return "", "", nil, err
	}

	where = joinWhere(filterSQL, cursorSQL)
	args = append(filterArgs, cursorArgs...)
	return where, orderSQL, args, nil
}

func joinWhere(filterSQL, cursorSQL string) string {
	switch {
	case filterSQL != "" && cursorSQL != "":
		return "(" + filterSQL + ") AND " + cursorSQL
	case filterSQL != "":
		return filterSQL
	case cursorSQL != "":
		return cursorSQL
	default:
		return ""
	}
}

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
	return pgxcel.Transpile(
		cel.CheckedExprToAst(f.CheckedExpr),
		pgxcel.WithColumns(columns),
		pgxcel.WithParamOffset(startParam),
		pgxcel.WithFunctions(aipFunctions),
	)
}

// aipFunctions normalizes the function names emitted by the
// einride/aip-go filter parser into the canonical cel-go names that
// pgxcel dispatches on. The AIP parser uses SQL-spelled comparison and
// logical operators, plus two AIP-only operators that map to CEL
// stdlib equivalents: ":" → string.contains, "FUZZY" → logical AND.
var aipFunctions = map[string]string{
	"=":     operators.Equals,
	"!=":    operators.NotEquals,
	"<":     operators.Less,
	"<=":    operators.LessEquals,
	">":     operators.Greater,
	">=":    operators.GreaterEquals,
	"AND":   operators.LogicalAnd,
	"OR":    operators.LogicalOr,
	"NOT":   operators.LogicalNot,
	"FUZZY": operators.LogicalAnd,
	":":     overloads.Contains,
}

// rewriteOrderBy translates an AIP-132 [ordering.OrderBy] into a
// comma-separated list of sanitized `col ASC|DESC` terms (no `ORDER BY`
// prefix). Returns "" when OrderBy has no fields.
//
// columns is the AIP-path → DB-column allow-list. Lookup is fail-closed:
// any path absent from columns causes an error.
func rewriteOrderBy(o ordering.OrderBy, columns map[string]string) (string, error) {
	if len(o.Fields) == 0 {
		return "", nil
	}
	parts := make([]string, 0, len(o.Fields))
	for _, f := range o.Fields {
		col, ok := columns[f.Path]
		if !ok {
			return "", fmt.Errorf("order: unknown field %q", f.Path)
		}
		dir := "ASC"
		if f.Desc {
			dir = "DESC"
		}
		parts = append(parts, sanitizePath(col)+" "+dir)
	}
	return strings.Join(parts, ", "), nil
}

// rewriteCursor builds the compound keyset predicate from an effective
// ordering and a matching slice of cursor values (the last row from the
// previous page). The emitted predicate is the standard tuple comparison,
// expanded to support per-column direction:
//
//	(c1 op1 $1)
//	  OR (c1 = $1 AND c2 op2 $2)
//	  OR (c1 = $1 AND c2 = $2 AND c3 op3 $3)
//	  ...
//
// where opN is `>` for ASC fields and `<` for DESC fields. Placeholders
// start at startParam.
//
// Returns ("", nil, nil) when values is empty (first-page request).
// Errors when len(fields) != len(values), or when any field's path is
// not in columns.
func rewriteCursor(
	fields []ordering.Field,
	values []any,
	columns map[string]string,
	startParam int,
) (string, []any, error) {
	if len(values) == 0 {
		return "", nil, nil
	}
	if len(fields) != len(values) {
		return "", nil, fmt.Errorf(
			"cursor: expected %d values for %d fields, got %d",
			len(fields), len(fields), len(values),
		)
	}
	cols := make([]string, len(fields))
	for i, f := range fields {
		col, ok := columns[f.Path]
		if !ok {
			return "", nil, fmt.Errorf("cursor: unknown field %q", f.Path)
		}
		cols[i] = sanitizePath(col)
	}

	clauses := make([]string, 0, len(fields))
	for i, f := range fields {
		parts := make([]string, 0, i+1)
		for j := 0; j < i; j++ {
			parts = append(parts, fmt.Sprintf("%s = $%d", cols[j], startParam+j))
		}
		op := ">"
		if f.Desc {
			op = "<"
		}
		parts = append(parts, fmt.Sprintf("%s %s $%d", cols[i], op, startParam+i))
		clauses = append(clauses, "("+strings.Join(parts, " AND ")+")")
	}
	return "(" + strings.Join(clauses, " OR ") + ")", append([]any(nil), values...), nil
}
