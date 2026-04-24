package pgxquery_test

import (
	"context"

	"github.com/jackc/pgx/v5"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pgx-contrib/pgxquery"
	"go.einride.tech/aip/filtering"
	"go.einride.tech/aip/ordering"
)

func parseFilter(expr string, opts ...filtering.DeclarationOption) filtering.Filter {
	GinkgoHelper()
	base := []filtering.DeclarationOption{filtering.DeclareStandardFunctions()}
	decls, err := filtering.NewDeclarations(append(base, opts...)...)
	Expect(err).NotTo(HaveOccurred())
	f, err := filtering.ParseFilterString(expr, decls)
	Expect(err).NotTo(HaveOccurred())
	return f
}

func parseOrderBy(s string) ordering.OrderBy {
	GinkgoHelper()
	var o ordering.OrderBy
	Expect(o.UnmarshalString(s)).To(Succeed())
	return o
}

var _ = Describe("Chain", func() {
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
	})

	It("strips the chain itself from args before running substituters", func() {
		chain := pgxquery.Chain{&pgxquery.FilterRewriter{}}
		sql, args, err := chain.RewriteQuery(ctx, nil,
			`WHERE /* query.filter AND */ TRUE`,
			[]any{chain, "keep"})
		Expect(err).NotTo(HaveOccurred())
		Expect(sql).To(Equal(`WHERE  TRUE`))
		Expect(args).To(Equal([]any{"keep"}))
	})

	It("applies substituters in order and threads args through", func() {
		cols := map[string]string{"name": "name"}
		chain := pgxquery.Chain{
			&pgxquery.FilterRewriter{
				Filter:  parseFilter(`name = "Alice"`, filtering.DeclareIdent("name", filtering.TypeString)),
				Columns: cols,
			},
			&pgxquery.OrderByRewriter{OrderBy: parseOrderBy("name desc"), Columns: cols},
		}
		sql, args, err := chain.RewriteQuery(ctx, nil,
			`SELECT * FROM t WHERE /* query.filter AND */ TRUE ORDER BY /* query.order , */ id`,
			[]any{chain, int32(10)})
		Expect(err).NotTo(HaveOccurred())
		Expect(sql).To(Equal(`SELECT * FROM t WHERE ("name" = $2) AND TRUE ORDER BY "name" DESC , id`))
		Expect(args).To(Equal([]any{int32(10), "Alice"}))
	})

	It("returns the query unchanged when no rewriters and no sentinels", func() {
		chain := pgxquery.Chain{}
		sql, args, err := chain.RewriteQuery(ctx, nil, "SELECT 1", []any{chain})
		Expect(err).NotTo(HaveOccurred())
		Expect(sql).To(Equal("SELECT 1"))
		Expect(args).To(BeEmpty())
	})

	It("wraps substituter errors with a pgxquery prefix", func() {
		chain := pgxquery.Chain{&errSubstituter{}}
		_, _, err := chain.RewriteQuery(ctx, nil, "SELECT 1", []any{chain})
		Expect(err).To(MatchError(ContainSubstring("pgxquery: boom")))
	})

	It("satisfies pgx.QueryRewriter", func() {
		var _ pgx.QueryRewriter = pgxquery.Chain{}
	})
})

type errSubstituter struct{}

func (errSubstituter) Substitute(string, []any) (string, []any, error) {
	return "", nil, errBoom
}

var errBoom = boomError{}

type boomError struct{}

func (boomError) Error() string { return "boom" }
