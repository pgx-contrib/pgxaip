package pgxaip

import (
	"fmt"
	"strings"

	"go.einride.tech/aip/ordering"
)

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
