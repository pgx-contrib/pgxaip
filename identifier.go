package pgxaip

import "strings"

// sanitizePath quotes each dot-separated segment of a column path as a
// Postgres identifier. Embedded double-quotes in a segment are escaped by
// doubling, matching pg_quote_ident semantics.
func sanitizePath(path string) string {
	parts := strings.Split(path, ".")
	out := make([]string, len(parts))
	for i, p := range parts {
		out[i] = `"` + strings.ReplaceAll(p, `"`, `""`) + `"`
	}
	return strings.Join(out, ".")
}
