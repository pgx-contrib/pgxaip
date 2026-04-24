package pgxquery

import (
	"fmt"

	"go.einride.tech/aip/filtering"
)

// FilterRewriter substitutes `/* query.filter ... */` sentinels with a WHERE
// predicate derived from an AIP-160 filter expression. If the filter is empty
// or unset, sentinels are removed and surrounding glue (`AND`, `OR`) that lives
// inside the comment disappears with them.
//
// Columns is the AIP path → DB column allow-list (e.g. the
// `*FilterColumns` map produced by `protoc-gen-go-aip-query`). Lookup is
// fail-closed: any field path in the filter that is not in Columns causes
// Substitute to return an error.
type FilterRewriter struct {
	Filter  filtering.Filter
	Columns map[string]string
}

// Substitute implements [Substituter].
func (r *FilterRewriter) Substitute(query string, args []any) (string, []any, error) {
	expr := r.Filter.CheckedExpr.GetExpr()
	if expr == nil {
		return replaceSentinel(query, "filter", ""), args, nil
	}
	t := &transpiler{args: args, columns: r.Columns}
	sql, err := t.transpile(expr)
	if err != nil {
		return "", nil, fmt.Errorf("filter: %w", err)
	}
	return replaceSentinel(query, "filter", "("+sql+")"), t.args, nil
}
