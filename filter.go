package pgxfilter

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"go.einride.tech/aip/filtering"
)

var _ pgx.QueryRewriter = &QueryRewriter{}

type QueryRewriter struct {
	expr string
}

func New(expr string) *QueryRewriter {
	return &QueryRewriter{
		expr: expr,
	}
}

// RewriteQuery implements [pgx.QueryRewriter].
func (q *QueryRewriter) RewriteQuery(_ context.Context, _ *pgx.Conn, query string, args []any) (string, []any, error) {
	if q.expr == "" {
		return query, args, nil
	}
	var p filtering.Parser
	p.Init(q.expr)
	parsed, err := p.Parse()
	if err != nil {
		return "", nil, fmt.Errorf("pgxfilter: %w", err)
	}
	if parsed.GetExpr() == nil {
		return query, args, nil
	}
	t := &transpiler{args: args}
	sql, err := t.transpile(parsed.GetExpr())
	if err != nil {
		return "", nil, fmt.Errorf("pgxfilter: %w", err)
	}
	return fmt.Sprintf("WITH query AS (%s) SELECT * FROM query WHERE %s", query, sql), t.args, nil
}
