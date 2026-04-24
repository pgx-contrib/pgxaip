// Package pgxaip provides [pgx.QueryRewriter] implementations that inject
// AIP-160 filter expressions, AIP-132 ordering, and keyset cursor predicates
// into SQL queries via comment-based sentinels.
//
// A SQL author marks injection points with `/* query.filter ... */`,
// `/* query.order ... */`, and `/* query.cursor ... */` comments. Without a
// rewriter, the comments act as whitespace and the query runs as-is.
//
// Each rewriter ([FilterRewriter], [OrderByRewriter], [CursorRewriter]) is a
// [pgx.QueryRewriter] on its own and can be passed as the first positional
// argument to pgx's query methods. Use [ChainRewriter] to compose multiple
// rewriters into a single pipeline.
package pgxaip

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

var _ pgx.QueryRewriter = ChainRewriter(nil)

// ChainRewriter is a [pgx.QueryRewriter] that runs a sequence of rewriters in
// order, threading the mutated query and bind args through each one.
//
// The ChainRewriter must be passed as the first positional argument to pgx's
// query methods; pgx detects the QueryRewriter and calls
// [ChainRewriter.RewriteQuery], which strips the ChainRewriter itself from
// the args before invoking each inner rewriter. Remaining args are the
// caller's bind values (e.g. LIMIT and OFFSET parameters); each inner
// rewriter appends any new bind values it needs to the end so existing
// placeholder numbers remain valid.
type ChainRewriter []pgx.QueryRewriter

// RewriteQuery implements [pgx.QueryRewriter].
//
// Follows the pgx convention: args[0] is expected to be the ChainRewriter
// itself and is stripped. Each inner rewriter is then invoked in the same
// convention — ChainRewriter prepends the inner rewriter to args so the
// rewriter's own self-strip removes it and only user bind values remain.
func (c ChainRewriter) RewriteQuery(ctx context.Context, conn *pgx.Conn, query string, args []any) (string, []any, error) {
	if len(args) > 0 {
		args = args[1:]
	}
	for _, r := range c {
		inner := make([]any, 0, len(args)+1)
		inner = append(inner, r)
		inner = append(inner, args...)
		var err error
		query, args, err = r.RewriteQuery(ctx, conn, query, inner)
		if err != nil {
			return "", nil, fmt.Errorf("pgxaip: %w", err)
		}
	}
	return query, args, nil
}
