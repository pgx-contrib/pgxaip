package pgxaip

import (
	"fmt"
	"strings"

	"go.einride.tech/aip/ordering"
)

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
