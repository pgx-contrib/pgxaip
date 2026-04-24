// Package pgxquery provides [pgx.QueryRewriter] implementations that inject
// AIP-160 filter expressions, AIP-132 ordering, and keyset cursor predicates
// into SQL queries via comment-based sentinels.
//
// A SQL author marks injection points with `/* query.filter ... */`,
// `/* query.order ... */`, and `/* query.cursor ... */` comments. Without a
// rewriter, the comments act as whitespace and the query runs as-is. With
// a [Chain] wired up, each [Substituter] replaces its sentinel with the
// corresponding SQL fragment.
package pgxquery

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Substituter rewrites a SQL fragment by replacing a single sentinel comment.
// Implementations receive the current query and bind args and return the
// transformed pair. They are composed with [Chain].
type Substituter interface {
	Substitute(query string, args []any) (string, []any, error)
}

var _ pgx.QueryRewriter = Chain{}

// Chain is a [pgx.QueryRewriter] that runs a sequence of [Substituter]s.
//
// The Chain must be passed as the first positional argument to pgx's
// query methods; pgx detects the QueryRewriter and calls [Chain.RewriteQuery],
// which strips the Chain itself from the args before invoking each inner
// Substituter in order. Remaining args are the caller's bind values (e.g.
// LIMIT and OFFSET parameters); each Substituter appends any new bind values
// it needs to the end so existing placeholder numbers remain valid.
type Chain []Substituter

// RewriteQuery implements [pgx.QueryRewriter].
func (c Chain) RewriteQuery(_ context.Context, _ *pgx.Conn, query string, args []any) (string, []any, error) {
	if len(args) > 0 {
		args = args[1:]
	}
	for _, s := range c {
		var err error
		query, args, err = s.Substitute(query, args)
		if err != nil {
			return "", nil, fmt.Errorf("pgxquery: %w", err)
		}
	}
	return query, args, nil
}
