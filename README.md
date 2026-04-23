# pgxquery

[![CI](https://github.com/pgx-contrib/pgxquery/actions/workflows/ci.yml/badge.svg)](https://github.com/pgx-contrib/pgxquery/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/pgx-contrib/pgxquery?include_prereleases)](https://github.com/pgx-contrib/pgxquery/releases)
[![Go Reference](https://pkg.go.dev/badge/github.com/pgx-contrib/pgxquery.svg)](https://pkg.go.dev/github.com/pgx-contrib/pgxquery)
[![License](https://img.shields.io/github/license/pgx-contrib/pgxquery)](LICENSE)
[![Go](https://img.shields.io/badge/Go-1.25-00ADD8?logo=go&logoColor=white)](https://go.dev)
[![pgx](https://img.shields.io/badge/pgx-v5-blue)](https://github.com/jackc/pgx)

`QueryRewriter` is a [pgx v5](https://github.com/jackc/pgx) query rewriter that
applies [AIP-160](https://google.aip.dev/160) filter expressions and
AIP-132 [`order_by`](https://google.aip.dev/132#ordering) strings to any SQL
query. Pass it as the last argument to `Query` and the filter becomes a
parameterised `WHERE` clause, the ordering becomes `ORDER BY` — no hand-rolled
SQL building.

See AIP-132 for how these fit into the [List method](https://google.aip.dev/132)
contract — [`filter`](https://google.aip.dev/132#filtering) and
[`order_by`](https://google.aip.dev/132#ordering) are the two request fields
this rewriter is designed to consume.

## Installation

```bash
go get github.com/pgx-contrib/pgxquery
```

## Usage

```go
import (
    "context"
    "os"

    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/pgx-contrib/pgxquery"
    "go.einride.tech/aip/filtering"
    "go.einride.tech/aip/ordering"
)

declarations, err := filtering.NewDeclarations(
    filtering.DeclareStandardFunctions(),
    filtering.DeclareIdent("first_name", filtering.TypeString),
    filtering.DeclareIdent("age", filtering.TypeInt),
)
if err != nil {
    panic(err)
}

filter, err := filtering.ParseFilterString(`first_name = "Alice" AND age > 30`, declarations)
if err != nil {
    panic(err)
}

var orderBy ordering.OrderBy
if err := orderBy.UnmarshalString("age desc, first_name"); err != nil {
    panic(err)
}

rewriter := pgxquery.New(filter, orderBy)

rows, err := pool.Query(context.TODO(), "SELECT * FROM customer", rewriter)
```

The rewritten SQL looks like:

```sql
WITH query AS (SELECT * FROM customer)
SELECT * FROM query
WHERE ("first_name" = $1 AND "age" > $2)
ORDER BY "age" DESC, "first_name" ASC
```

### How it works

The input query is wrapped as a CTE so filtering and ordering can be applied
without parsing the SQL. This keeps the rewriter agnostic to the query shape —
any valid `SELECT` works, including joins and sub-queries — but the planner must
materialize the CTE before applying `WHERE`/`ORDER BY`. For very large tables
where you need predicate pushdown, inline the filter yourself.

### Supported filter functions

| AIP filter                  | SQL                        |
| --------------------------- | -------------------------- |
| `a = b`, `!=`, `<`, `<=`, `>`, `>=` | `a <op> b`          |
| `a AND b`                   | `(a AND b)`                |
| `a OR b`                    | `(a OR b)`                 |
| `NOT a`                     | `(NOT a)`                  |
| `field:value` (has)         | `field ILIKE '%' \|\| $n \|\| '%'` |

Identifiers and dotted paths (e.g. `address.city`) are sanitised with
`pgx.Identifier.Sanitize`; constants are always emitted as bound parameters.

### Empty rewriter

A `QueryRewriter{}` with no filter or ordering is a no-op — the query passes
through unchanged. This makes it safe to wire up unconditionally and populate
the fields from request inputs.

## Development

### DevContainer

Open in VS Code with the Dev Containers extension. The environment provides Go,
PostgreSQL 18, and Nix automatically.

```
PGX_DATABASE_URL=postgres://vscode@postgres:5432/pgxquery?sslmode=disable
```

### Nix

```bash
nix develop          # enter shell with Go
go tool ginkgo run -r
```

### Run tests

```bash
go tool ginkgo run -r
```

## License

[MIT](LICENSE)
