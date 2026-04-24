# pgxquery

[![CI](https://github.com/pgx-contrib/pgxquery/actions/workflows/ci.yml/badge.svg)](https://github.com/pgx-contrib/pgxquery/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/pgx-contrib/pgxquery?include_prereleases)](https://github.com/pgx-contrib/pgxquery/releases)
[![Go Reference](https://pkg.go.dev/badge/github.com/pgx-contrib/pgxquery.svg)](https://pkg.go.dev/github.com/pgx-contrib/pgxquery)
[![License](https://img.shields.io/github/license/pgx-contrib/pgxquery)](LICENSE)
[![Go](https://img.shields.io/badge/Go-1.25-00ADD8?logo=go&logoColor=white)](https://go.dev)
[![pgx](https://img.shields.io/badge/pgx-v5-blue)](https://github.com/jackc/pgx)

`pgxquery` is a set of [pgx v5](https://github.com/jackc/pgx) query rewriters
that inject dynamic [AIP-160](https://google.aip.dev/160) filter expressions,
[AIP-132 order_by](https://google.aip.dev/132#ordering), and keyset cursor
predicates into SQL — using comment-based sentinels that keep the raw query
valid and runnable on its own.

```sql
SELECT * FROM collection
WHERE /* query.cursor AND */ TRUE
  AND /* query.filter AND */ TRUE
ORDER BY /* query.order , */ collection_id
LIMIT $1::int OFFSET $2::int;
```

Without a rewriter, the comments act as whitespace and the query returns every
row. With a [`Chain`](https://pkg.go.dev/github.com/pgx-contrib/pgxquery#Chain)
wired up, each sentinel gets swapped for the corresponding SQL fragment.

## Installation

```bash
go get github.com/pgx-contrib/pgxquery
```

## Usage

```go
import (
    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/pgx-contrib/pgxquery"
    "go.einride.tech/aip/filtering"
    "go.einride.tech/aip/ordering"
)

// Parse request inputs against typed declarations.
declarations, _ := filtering.NewDeclarations(
    filtering.DeclareStandardFunctions(),
    filtering.DeclareIdent("display_name", filtering.TypeString),
)
filter, _ := filtering.ParseFilterString(`display_name = "Photos"`, declarations)

var orderBy ordering.OrderBy
_ = orderBy.UnmarshalString("display_name desc")

// The cursor must know the effective ordering — user fields plus any
// SQL-level fallback (the PK, typically).
effective := append(orderBy.Fields, ordering.Field{Path: "collection_id"})

chain := pgxquery.Chain{
    &pgxquery.FilterRewriter{
        Filter:  filter,
        Columns: templatev1.CollectionFilterColumns,
    },
    &pgxquery.OrderByRewriter{
        OrderBy: orderBy,
        Columns: templatev1.CollectionOrderByColumns,
    },
    &pgxquery.CursorRewriter{
        Fields:  effective,
        Values:  decodePageToken(req.PageToken),
        Columns: templatev1.CollectionOrderByColumns,
    },
}

rows, err := pool.Query(ctx, listCollectionsSQL, chain, req.PageSize, req.PageOffset)
```

`Columns` is the AIP-path → DB-column allow-list. Pair it with the
`*FilterColumns` / `*OrderByColumns` maps produced by
`protoc-gen-go-aip-query`, or hand-build a `map[string]string` for
ad-hoc use. Lookup is **fail-closed** — any filter/order/cursor path
that is not in `Columns` causes the substituter to return an error, so
an unmapped field can never leak into generated SQL.

The `Chain` must be passed as the **first** positional argument — pgx detects
the `QueryRewriter` interface and calls `RewriteQuery`. Remaining args stay as
your query's bind values (`LIMIT`, `OFFSET`, etc.); each substituter appends
any new values at the end so placeholder numbers remain stable.

## Sentinels

| Comment                       | What it becomes                                               |
| ----------------------------- | ------------------------------------------------------------- |
| `/* query.filter AND */ TRUE` | `(<expr>) AND TRUE`, or `TRUE` when the filter is unset       |
| `/* query.order , */ id`      | `<fields>, id`, or just `id` when the order is unset          |
| `/* query.cursor AND */ TRUE` | compound keyset predicate `AND TRUE`, or `TRUE` on first page |

**Glue position is flexible** — put it before or after the sentinel name, as
long as it lives inside the comment. These are equivalent:

```sql
/* query.filter AND */ fallback
fallback /* AND query.filter */
```

## Keyset cursors

`CursorRewriter` emits the standard compound keyset predicate so pagination
stays correct across multi-column ordering:

```sql
(name < $1)
  OR (name = $1 AND id > $2)
```

The `Fields` list should include every column that contributes to ordering —
both user-supplied and whatever the SQL author wrote as a tiebreaker. Values
are the decoded page token (one per field); pass `nil` on the first page.

## Writing queries

- Comment sentinels degrade to whitespace, so the query is valid SQL with or
  without a rewriter. Paste it into `psql` to debug.
- Glue tokens (`AND`, `OR`, `,`) go **inside** the comment; the surrounding SQL
  must be a valid expression on its own if the rewriter is absent.
- The sentinel always expands in-place — the filter predicate lives in the
  same `WHERE` clause as your cursor and `LIMIT`, so the planner filters
  *before* paginating (unlike CTE-wrapping approaches).

## Development

### DevContainer

Open in VS Code with the Dev Containers extension. Go, PostgreSQL 18, and Nix
come preconfigured.

```
PGX_DATABASE_URL=postgres://vscode@postgres:5432/pgxquery?sslmode=disable
```

### Nix

```bash
nix develop          # enter shell with Go
go tool ginkgo run -r
```

## License

[MIT](LICENSE)
