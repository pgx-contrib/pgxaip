package pgxquery

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"go.einride.tech/aip/filtering"
	"go.einride.tech/aip/ordering"
)

var _ pgx.QueryRewriter = &QueryRewriter{}

// QueryRewriter is a [pgx.QueryRewriter] that applies AIP-160 filter and
// AIP-132 ordering expressions to any SQL query.
type QueryRewriter struct {
	Filter  filtering.Filter
	OrderBy ordering.OrderBy
}

// New returns a [QueryRewriter] for the given filter and ordering.
func New(filter filtering.Filter, orderBy ordering.OrderBy) *QueryRewriter {
	return &QueryRewriter{Filter: filter, OrderBy: orderBy}
}

// RewriteQuery implements [pgx.QueryRewriter].
//
// The input query is wrapped as a CTE so filtering and ordering can be applied
// without parsing the SQL. This keeps the rewriter agnostic to the query shape,
// but the planner must materialize the CTE before applying WHERE/ORDER BY.
func (q *QueryRewriter) RewriteQuery(_ context.Context, _ *pgx.Conn, query string, args []any) (string, []any, error) {
	t := &transpiler{args: args}

	var where string
	if expr := q.Filter.CheckedExpr.GetExpr(); expr != nil {
		sql, err := t.transpile(expr)
		if err != nil {
			return "", nil, fmt.Errorf("pgxquery: %w", err)
		}
		where = sql
	}

	var orderBy string
	if len(q.OrderBy.Fields) > 0 {
		parts := make([]string, 0, len(q.OrderBy.Fields))
		for _, f := range q.OrderBy.Fields {
			direction := "ASC"
			if f.Desc {
				direction = "DESC"
			}
			parts = append(parts, sanitizePath(f.Path)+" "+direction)
		}
		orderBy = strings.Join(parts, ", ")
	}

	if where == "" && orderBy == "" {
		return query, args, nil
	}

	var b strings.Builder
	fmt.Fprintf(&b, "WITH query AS (%s) SELECT * FROM query", query)
	if where != "" {
		fmt.Fprintf(&b, " WHERE %s", where)
	}
	if orderBy != "" {
		fmt.Fprintf(&b, " ORDER BY %s", orderBy)
	}
	return b.String(), t.args, nil
}

func sanitizePath(path string) string {
	parts := strings.Split(path, ".")
	out := make([]string, len(parts))
	for i, p := range parts {
		out[i] = pgx.Identifier{p}.Sanitize()
	}
	return strings.Join(out, ".")
}
