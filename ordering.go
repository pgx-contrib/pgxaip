package pgxaip

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"go.einride.tech/aip/ordering"
)

var _ pgx.QueryRewriter = (*OrderByRewriter)(nil)

// OrderByRewriter substitutes `/* query.order ... */` sentinels with a
// comma-separated list of sanitized `col ASC|DESC` terms derived from an
// AIP-132 `order_by` value. If [OrderBy] has no fields, sentinels are removed.
//
// Columns is the AIP path → DB column allow-list (e.g. the
// `*OrderByColumns` map produced by `protoc-gen-go-aip-query`). Lookup is
// fail-closed: any field path in OrderBy that is not in Columns causes
// RewriteQuery to return an error.
type OrderByRewriter struct {
	OrderBy ordering.OrderBy
	Columns map[string]string
}

// RewriteQuery implements [pgx.QueryRewriter]. OrderByRewriter can be used
// standalone as the first positional arg to pgx's query methods, or composed
// via [ChainRewriter].
//
// Follows the pgx convention: args[0] is expected to be the rewriter itself
// and is stripped before processing. Callers who invoke this directly (tests,
// [ChainRewriter]) must pass args in that form.
func (r *OrderByRewriter) RewriteQuery(_ context.Context, _ *pgx.Conn, query string, args []any) (string, []any, error) {
	if len(args) > 0 {
		args = args[1:]
	}
	if len(r.OrderBy.Fields) == 0 {
		return replaceSentinel(query, "order", ""), args, nil
	}
	parts := make([]string, 0, len(r.OrderBy.Fields))
	for _, f := range r.OrderBy.Fields {
		col, ok := r.Columns[f.Path]
		if !ok {
			return "", nil, fmt.Errorf("order: unknown field %q", f.Path)
		}
		dir := "ASC"
		if f.Desc {
			dir = "DESC"
		}
		parts = append(parts, sanitizePath(col)+" "+dir)
	}
	return replaceSentinel(query, "order", strings.Join(parts, ", ")), args, nil
}
