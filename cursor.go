package pgxquery

import (
	"fmt"
	"strings"

	"go.einride.tech/aip/ordering"
)

// CursorRewriter substitutes `/* query.cursor ... */` sentinels with a
// compound keyset predicate derived from the effective ordering and a
// decoded page token.
//
// [Fields] is the full effective ordering — user-supplied fields plus any
// SQL-level fallback (typically the primary key as the trailing tiebreaker).
// [Values] is the decoded page token, one entry per field, representing the
// last row from the previous page. Its length must equal len([Fields]).
//
// [Columns] is the AIP path → DB column allow-list (typically the same
// `*OrderByColumns` map you pass to [OrderByRewriter] — cursor field paths
// must resolve through the same allow-list as the ordering). Lookup is
// fail-closed: any field path in Fields that is not in Columns causes
// Substitute to return an error.
//
// The emitted predicate is the standard "greater than" tuple comparison,
// expanded to support per-column direction:
//
//	(c1 op1 v1)
//	  OR (c1 = v1 AND c2 op2 v2)
//	  OR (c1 = v1 AND c2 = v2 AND c3 op3 v3)
//	  ...
//
// where opN is `>` for ASC fields and `<` for DESC fields.
//
// If [Values] is empty, the sentinel is removed (first-page request).
type CursorRewriter struct {
	Fields  []ordering.Field
	Values  []any
	Columns map[string]string
}

// Substitute implements [Substituter].
func (r *CursorRewriter) Substitute(query string, args []any) (string, []any, error) {
	if len(r.Values) == 0 {
		return replaceSentinel(query, "cursor", ""), args, nil
	}
	if len(r.Fields) != len(r.Values) {
		return "", nil, fmt.Errorf("cursor: expected %d values for %d fields, got %d",
			len(r.Fields), len(r.Fields), len(r.Values))
	}

	cols := make([]string, len(r.Fields))
	for i, f := range r.Fields {
		col, ok := r.Columns[f.Path]
		if !ok {
			return "", nil, fmt.Errorf("cursor: unknown field %q", f.Path)
		}
		cols[i] = sanitizePath(col)
	}

	startParam := len(args) + 1
	clauses := make([]string, 0, len(r.Fields))
	for i, f := range r.Fields {
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

	fragment := "(" + strings.Join(clauses, " OR ") + ")"
	return replaceSentinel(query, "cursor", fragment), append(args, r.Values...), nil
}
