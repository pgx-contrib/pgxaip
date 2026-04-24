# pgxaip

[![CI](https://github.com/pgx-contrib/pgxaip/actions/workflows/ci.yml/badge.svg)](https://github.com/pgx-contrib/pgxaip/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/pgx-contrib/pgxaip?include_prereleases)](https://github.com/pgx-contrib/pgxaip/releases)
[![Go Reference](https://pkg.go.dev/badge/github.com/pgx-contrib/pgxaip.svg)](https://pkg.go.dev/github.com/pgx-contrib/pgxaip)
[![License](https://img.shields.io/github/license/pgx-contrib/pgxaip)](LICENSE)
[![Go](https://img.shields.io/badge/Go-1.25-00ADD8?logo=go&logoColor=white)](https://go.dev)

`pgxaip` rewrites a parsed [AIP-160](https://google.aip.dev/160) filter,
an [AIP-132](https://google.aip.dev/132#ordering) `order_by`, and an
optional keyset cursor into Postgres SQL fragments you splice into a
query by hand.

```go
query := pgxaip.Query{
    Filter:    filter,     // from filtering.ParseFilter
    OrderBy:   orderBy,    // from ordering.ParseOrderBy
    PageToken: pageToken,  // from pagination.ParsePageToken (incl. Cursor)
    Columns:   columns,    // AIP path -> DB column
}

where, order, args, err := query.Rewrite()
```

- `where` — the WHERE predicate (filter, cursor, or both ANDed). Empty
  when neither is present.
- `order` — the `col ASC, col DESC` list, no `ORDER BY` prefix. Empty
  when `OrderBy` has no fields.
- `args` — positional bind values, numbered `$1..$N`. Filter literals
  first, then cursor values. Append your own `LIMIT` / `OFFSET` at
  `$N+1`.

`PageToken.Offset` is not consulted by `Rewrite`; feed it into your
`OFFSET` clause yourself.

## Installation

```bash
go get github.com/pgx-contrib/pgxaip
```

## Usage

```go
filter, _ := filtering.ParseFilter(req, BookFilterDeclarations)
orderBy, _ := ordering.ParseOrderBy(req)
pageToken, _ := pagination.ParsePageToken(req)

// Unified column map: union of the generated *FilterColumns and
// *OrderByColumns. Same path -> same column by construction, so the
// union is safe.
columns := map[string]string{}
for k, v := range BookFilterColumns {
    columns[k] = v
}
for k, v := range BookOrderByColumns {
    columns[k] = v
}

q := pgxaip.Query{
    Filter:    filter,
    OrderBy:   orderBy,
    PageToken: pageToken,
    Columns:   columns,
}
where, order, args, err := q.Rewrite()
```

`Columns` is the AIP-path → DB-column allow-list. Lookup is
**fail-closed**: any filter / order / cursor path that is not in
`Columns` causes `Rewrite` to return an error, so an unmapped field
can never leak into generated SQL. Pair with the `*FilterColumns` /
`*OrderByColumns` maps produced by
[`protoc-gen-go-aip-query`](https://github.com/protoc-contrib/protoc-gen-go-aip-query)
or hand-build a `map[string]string` for ad-hoc use.

## Filter operators

| AIP filter                          | Postgres fragment                     |
| ----------------------------------- | ------------------------------------- |
| `=, !=, <, <=, >, >=`               | `col op $N` (or `col op col`)         |
| `AND`, `OR`                         | `(lhs AND rhs)` / `(lhs OR rhs)`      |
| `NOT`                               | `(NOT expr)`                          |
| `name:"ali"`                        | `"name" ILIKE '%' \|\| $N \|\| '%'`   |
| `timestamp("2025-01-02T03:04:05Z")` | `$N` bound as `time.Time`             |
| `duration("1h30m")`                 | `$N` bound as `time.Duration`         |
| unary `-<literal>`                  | bound as signed numeric literal       |

## Cursor pagination

When `PageToken.Cursor` is populated, `Rewrite` emits the standard
compound keyset predicate from `OrderBy.Fields`:

```sql
("name" < $1)
  OR ("name" = $1 AND "id" > $2)
```

Direction per field follows the `OrderBy` field's `Desc` flag
(ASC → `>`, DESC → `<`). `len(PageToken.Cursor)` must equal
`len(OrderBy.Fields)`; mismatch is a validation error.

For a stable ordering, append a tiebreaker (the PK) to
`OrderBy.Fields` before calling `Rewrite`, and make sure
`PageToken.Cursor` carries a matching trailing value.

On the first page (`PageToken.Cursor` is empty) the cursor predicate
is omitted.

## Development

```bash
nix develop
go tool ginkgo run -r
```

## License

[MIT](LICENSE)
