// Package pgxaip rewrites AIP-160 filter, AIP-132 order_by, and keyset
// cursor values into Postgres SQL fragments that the caller splices into
// a query by hand.
//
// The entry point is [Query]. Populate it from the request parsers (for
// example the ones emitted by protoc-gen-go-aip-query) and call
// [Query.Rewrite] to produce the WHERE predicate, the ORDER BY list, and
// the matching positional args. Pagination is offset- or cursor-based —
// driven by [pagination.PageToken.Cursor] when set — and the caller
// tacks the LIMIT/OFFSET pair onto the SQL themselves.
package pgxaip

import (
	"go.einride.tech/aip/filtering"
	"go.einride.tech/aip/ordering"
	"go.einride.tech/aip/pagination"
)

// Query bundles the parsed AIP inputs for a List request together with
// the AIP-path → DB-column allow-list that the rewriter uses to resolve
// identifiers. Any path in Filter or OrderBy that is not in Columns
// causes [Query.Rewrite] to fail.
//
// OrderBy is the effective ordering — user-supplied fields plus any
// tiebreaker (the primary key, typically) the caller appends. When
// PageToken.Cursor is non-empty, its length must match len(OrderBy.Fields).
type Query struct {
	Filter    filtering.Filter
	OrderBy   ordering.OrderBy
	PageToken pagination.PageToken
	Columns   map[string]string
}

// Rewrite returns the SQL fragments and bound args derived from the
// query's filter, ordering, and cursor.
//
//   - where  is the WHERE predicate: the filter expression, the keyset
//     cursor predicate, or the two ANDed together. Empty string when
//     neither is present — the caller should omit the WHERE keyword.
//   - order  is the comma-separated ORDER BY list (no "ORDER BY" prefix).
//     Empty string when OrderBy has no fields.
//   - args   are the positional bind values, numbered $1..$N. Filter
//     literals come first, then cursor values. The caller appends its
//     own LIMIT/OFFSET values at $N+1 and onward.
//
// PageToken.Offset is not consulted; the caller feeds it into an OFFSET
// clause themselves.
func (q Query) Rewrite() (where, order string, args []any, err error) {
	filterSQL, filterArgs, err := rewriteFilter(q.Filter, q.Columns, 1)
	if err != nil {
		return "", "", nil, err
	}

	cursorSQL, cursorArgs, err := rewriteCursor(
		q.OrderBy.Fields,
		q.PageToken.Cursor,
		q.Columns,
		1+len(filterArgs),
	)
	if err != nil {
		return "", "", nil, err
	}

	orderSQL, err := rewriteOrderBy(q.OrderBy, q.Columns)
	if err != nil {
		return "", "", nil, err
	}

	where = joinWhere(filterSQL, cursorSQL)
	args = append(filterArgs, cursorArgs...)
	return where, orderSQL, args, nil
}

func joinWhere(filterSQL, cursorSQL string) string {
	switch {
	case filterSQL != "" && cursorSQL != "":
		return "(" + filterSQL + ") AND " + cursorSQL
	case filterSQL != "":
		return filterSQL
	case cursorSQL != "":
		return cursorSQL
	default:
		return ""
	}
}
