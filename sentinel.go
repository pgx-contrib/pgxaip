package pgxquery

import (
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5"
)

// sanitizePath applies [pgx.Identifier.Sanitize] to each dot-separated segment
// of a dotted column path (e.g. `address.city`).
func sanitizePath(path string) string {
	parts := strings.Split(path, ".")
	out := make([]string, len(parts))
	for i, p := range parts {
		out[i] = pgx.Identifier{p}.Sanitize()
	}
	return strings.Join(out, ".")
}

// sentinelRegex builds a regex that matches `/* [pre] query.<name> [post] */`
// comments. Capture groups are:
//
//	1: pre-glue text (whitespace-trimmed, may be empty)
//	2: post-glue text (whitespace-trimmed, may be empty)
func sentinelRegex(name string) *regexp.Regexp {
	return regexp.MustCompile(`/\*\s*([^*]*?)\s*query\.` + regexp.QuoteMeta(name) + `\b\s*([^*]*?)\s*\*/`)
}

// replaceSentinel swaps every `/* ... query.<name> ... */` comment in query
// with pre-glue, the supplied fragment, and post-glue (each separated by a
// single space). If fragment is empty, the comment is removed entirely.
func replaceSentinel(query, name, fragment string) string {
	re := sentinelRegex(name)
	return re.ReplaceAllStringFunc(query, func(match string) string {
		sub := re.FindStringSubmatch(match)
		if sub == nil {
			return match
		}
		if fragment == "" {
			return ""
		}
		var parts []string
		if sub[1] != "" {
			parts = append(parts, sub[1])
		}
		parts = append(parts, fragment)
		if sub[2] != "" {
			parts = append(parts, sub[2])
		}
		return strings.Join(parts, " ")
	})
}
