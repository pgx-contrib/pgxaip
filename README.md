# pgxaip

[![CI](https://github.com/pgx-contrib/pgxaip/actions/workflows/ci.yml/badge.svg)](https://github.com/pgx-contrib/pgxaip/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/pgx-contrib/pgxaip?include_prereleases)](https://github.com/pgx-contrib/pgxaip/releases)
[![Go Reference](https://pkg.go.dev/badge/github.com/pgx-contrib/pgxaip.svg)](https://pkg.go.dev/github.com/pgx-contrib/pgxaip)
[![License](https://img.shields.io/github/license/pgx-contrib/pgxaip)](LICENSE)
[![Go](https://img.shields.io/badge/Go-1.25-00ADD8?logo=go&logoColor=white)](https://go.dev)

`pgxaip` rewrites a compiled [CEL](https://github.com/google/cel-go) filter,
an [AIP-132](https://google.aip.dev/132#ordering) `order_by`, and an
optional keyset cursor into Postgres SQL fragments you splice into a
query by hand.

```go
query := pgxaip.Query{
    Filter:    ast,        // *cel.Ast, already compiled and checked
    OrderBy:   orderBy,    // aip.OrderBy, already parsed and validated
    PageToken: pageToken,  // aip.PageToken; only .Cursor is read
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

`pgxaip` parses nothing and validates no grammar. Filters arrive as an
already-checked `*cel.Ast`, ordering as an already-parsed `aip.OrderBy`.
Whatever produced them is where request-level errors belong. `pgxaip` owns
exactly one thing: turning those values into SQL text safely.

## Installation

```bash
go get github.com/pgx-contrib/pgxaip
```

## Usage

`Query` mirrors the `Query` that
[`protoc-gen-go-aip`](https://github.com/protoc-contrib/protoc-gen-go-aip)
generates, so wiring the two together is a field-by-field copy plus the
column map:

```go
q, err := request.ParseQuery() // generated: compiles the filter, parses
if err != nil {                // order_by and page_token, validates each
    return nil, status.Error(codes.InvalidArgument, err.Error())
}

where, order, args, err := pgxaip.Query{
    Filter:    q.Filter,    // *cel.Ast, compiled against ListBooksFilterEnv
    OrderBy:   q.OrderBy,   // aip.OrderBy, validated against ListBooksOrderByFields
    PageToken: q.PageToken, // aip.PageToken
    Columns:   BookColumns,
}.Rewrite()
```

You can equally compile the filter yourself — `pgxaip` accepts any
`*cel.Ast` and any `aip.OrderBy`, however they were produced:

```go
env, _ := cel.NewEnv(cel.Variable("name", cel.StringType))
ast, issues := env.Compile(`name.contains("ali")`)
if issues.Err() != nil {
    return nil, status.Error(codes.InvalidArgument, issues.Err().Error())
}
```

`Columns` is the AIP-path → DB-column allow-list, and you build it by hand —
the generator emits `ListBooksOrderByFields []string` and a `*cel.Env`, not a
column map:

```go
var BookColumns = map[string]string{
    "name":        "name",
    "create_time": "created_at",
    "id":          "id",
}
```

Lookup is **fail-closed**: any filter / order / cursor path that is not in
`Columns` causes `Rewrite` to return an error, so an unmapped field can never
leak into generated SQL. This is the *only* gate on what a client may filter
or sort by — the generated CEL environment declares every field of the
resource, so it will happily compile an expression over a column you never
meant to expose.

Feed `where` and `order` into
[`pgxquery`](https://github.com/pgx-contrib/pgxquery) to splice them into your
SQL at sentinel comments, or interpolate them yourself.

## Filter operators

The filter is a `cel-go` AST, so the syntax is CEL. Translation is done by
[`pgxcel`](https://github.com/pgx-contrib/pgxcel):

| CEL expression                      | Postgres fragment                     |
| ----------------------------------- | ------------------------------------- |
| `==, !=, <, <=, >, >=`              | `col op $N` (or `col op col`)         |
| `&&`, `\|\|`                        | `(lhs AND rhs)` / `(lhs OR rhs)`      |
| `!(expr)`                           | `(NOT expr)`                          |
| `name.contains("ali")`              | `"name" LIKE '%' \|\| $N \|\| '%'`    |
| `name.startsWith("a")`              | `"name" LIKE $N \|\| '%'`             |
| `name.endsWith("z")`                | `"name" LIKE '%' \|\| $N`             |
| `name.matches("^a.*z$")`            | `"name" ~ $N`                         |
| `name in ["a", "b"]`                | `"name" IN ($N, $N+1)`                |
| `timestamp("2025-01-02T03:04:05Z")` | `$N` bound as `time.Time`             |
| `duration("1h30m")`                 | `$N` bound as `time.Duration`         |
| unary `-<literal>`                  | bound as signed numeric literal       |

Note this is CEL, not [AIP-160](https://google.aip.dev/160) filter syntax.
AIP-160 spells these `=`, `AND`, `NOT`, and `name:"ali"`; an AST from an
AIP-160 parser carries those function names and would need
`pgxcel.WithFunctions` to normalize them, which `pgxaip` does not apply.

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
`PageToken.Cursor` carries a matching trailing value — build the next
token with `aip.PageToken.NextCursor(row, orderBy.Paths()...)`.

On the first page (`PageToken.Cursor` is empty) the cursor predicate
is omitted.

## Development

```bash
nix develop
go tool ginkgo run -r
```

## License

[MIT](LICENSE)
