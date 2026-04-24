package pgxaip_test

import (
	"context"

	"github.com/jackc/pgx/v5"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pgx-contrib/pgxaip"
	"go.einride.tech/aip/filtering"
)

var _ = Describe("ChainRewriter", func() {
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
	})

	It("strips the chain itself from args before running rewriters", func() {
		chain := pgxaip.ChainRewriter{&pgxaip.FilterRewriter{}}
		sql, args, err := chain.RewriteQuery(ctx, nil,
			`WHERE /* query.filter AND */ TRUE`,
			[]any{chain, "keep"})
		Expect(err).NotTo(HaveOccurred())
		Expect(sql).To(Equal(`WHERE  TRUE`))
		Expect(args).To(Equal([]any{"keep"}))
	})

	It("applies rewriters in order and threads args through", func() {
		cols := map[string]string{"name": "name"}
		chain := pgxaip.ChainRewriter{
			&pgxaip.FilterRewriter{
				Filter:  parseFilter(`name = "Alice"`, filtering.DeclareIdent("name", filtering.TypeString)),
				Columns: cols,
			},
			&pgxaip.OrderByRewriter{OrderBy: parseOrderBy("name desc"), Columns: cols},
		}
		sql, args, err := chain.RewriteQuery(ctx, nil,
			`SELECT * FROM t WHERE /* query.filter AND */ TRUE ORDER BY /* query.order , */ id`,
			[]any{chain, int32(10)})
		Expect(err).NotTo(HaveOccurred())
		Expect(sql).To(Equal(`SELECT * FROM t WHERE ("name" = $2) AND TRUE ORDER BY "name" DESC , id`))
		Expect(args).To(Equal([]any{int32(10), "Alice"}))
	})

	It("does not let inner rewriters re-strip args", func() {
		// When ChainRewriter calls an inner rewriter, args[0] is a user bind
		// value, not the inner rewriter. The inner rewriter's self-strip
		// must be a no-op, so the user's bind value survives.
		cols := map[string]string{"name": "name"}
		chain := pgxaip.ChainRewriter{
			&pgxaip.FilterRewriter{
				Filter:  parseFilter(`name = "Alice"`, filtering.DeclareIdent("name", filtering.TypeString)),
				Columns: cols,
			},
		}
		sql, args, err := chain.RewriteQuery(ctx, nil,
			`WHERE /* query.filter AND */ TRUE AND other = $1`,
			[]any{chain, "keep"})
		Expect(err).NotTo(HaveOccurred())
		Expect(sql).To(ContainSubstring(`"name" = $2`))
		Expect(args).To(Equal([]any{"keep", "Alice"}))
	})

	It("returns the query unchanged when no rewriters and no sentinels", func() {
		chain := pgxaip.ChainRewriter{}
		sql, args, err := chain.RewriteQuery(ctx, nil, "SELECT 1", []any{chain})
		Expect(err).NotTo(HaveOccurred())
		Expect(sql).To(Equal("SELECT 1"))
		Expect(args).To(BeEmpty())
	})

	It("wraps inner rewriter errors with a pgxaip prefix", func() {
		chain := pgxaip.ChainRewriter{&errRewriter{}}
		_, _, err := chain.RewriteQuery(ctx, nil, "SELECT 1", []any{chain})
		Expect(err).To(MatchError(ContainSubstring("pgxaip: boom")))
	})

	It("satisfies pgx.QueryRewriter", func() {
		var _ pgx.QueryRewriter = pgxaip.ChainRewriter{}
	})
})

type errRewriter struct{}

func (*errRewriter) RewriteQuery(context.Context, *pgx.Conn, string, []any) (string, []any, error) {
	return "", nil, errBoom
}

var errBoom = boomError{}

type boomError struct{}

func (boomError) Error() string { return "boom" }
