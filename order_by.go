package pgxquery

import (
	"fmt"
	"strings"

	"go.einride.tech/aip/ordering"
)

// OrderByRewriter substitutes `/* query.order ... */` sentinels with a
// comma-separated list of sanitized `col ASC|DESC` terms derived from an
// AIP-132 `order_by` value. If [OrderBy] has no fields, sentinels are removed.
//
// Columns is the AIP path → DB column allow-list (e.g. the
// `*OrderByColumns` map produced by `protoc-gen-go-aip-query`). Lookup is
// fail-closed: any field path in OrderBy that is not in Columns causes
// Substitute to return an error.
type OrderByRewriter struct {
	OrderBy ordering.OrderBy
	Columns map[string]string
}

// Substitute implements [Substituter].
func (r *OrderByRewriter) Substitute(query string, args []any) (string, []any, error) {
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
