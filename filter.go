package pgxaip

import (
	"github.com/google/cel-go/cel"
	"github.com/pgx-contrib/pgxcel"
	"go.einride.tech/aip/filtering"
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
	return pgxcel.Transpile(
		cel.CheckedExprToAst(f.CheckedExpr),
		pgxcel.WithColumns(columns),
		pgxcel.WithParamOffset(startParam),
	)
}
