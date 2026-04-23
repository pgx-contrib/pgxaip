package pgxquery_test

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgx-contrib/pgxquery"
	"go.einride.tech/aip/filtering"
	"go.einride.tech/aip/ordering"
)

func ExampleQueryRewriter() {
	config, err := pgxpool.ParseConfig(os.Getenv("PGX_DATABASE_URL"))
	if err != nil {
		panic(err)
	}

	pool, err := pgxpool.NewWithConfig(context.TODO(), config)
	if err != nil {
		panic(err)
	}
	defer pool.Close()

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
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	fmt.Println("ok")
}
