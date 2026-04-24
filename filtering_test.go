package pgxaip_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pgx-contrib/pgxaip"
	"go.einride.tech/aip/filtering"
)

var _ = Describe("FilterRewriter", func() {
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
	})

	It("substitutes a simple equality predicate", func() {
		r := &pgxaip.FilterRewriter{
			Filter:  parseFilter(`name = "Alice"`, filtering.DeclareIdent("name", filtering.TypeString)),
			Columns: map[string]string{"name": "name"},
		}
		sql, args, err := r.RewriteQuery(ctx, nil, `WHERE /* query.filter AND */ TRUE`, []any{r})
		Expect(err).NotTo(HaveOccurred())
		Expect(sql).To(Equal(`WHERE ("name" = $1) AND TRUE`))
		Expect(args).To(Equal([]any{"Alice"}))
	})

	It("maps AIP paths to their backing DB columns", func() {
		r := &pgxaip.FilterRewriter{
			Filter:  parseFilter(`create_time = "2025-01-01"`, filtering.DeclareIdent("create_time", filtering.TypeString)),
			Columns: map[string]string{"create_time": "created_at"},
		}
		sql, args, err := r.RewriteQuery(ctx, nil, `WHERE /* query.filter AND */ TRUE`, []any{r})
		Expect(err).NotTo(HaveOccurred())
		Expect(sql).To(Equal(`WHERE ("created_at" = $1) AND TRUE`))
		Expect(args).To(Equal([]any{"2025-01-01"}))
	})

	It("supports trailing-position sentinel with inside glue", func() {
		r := &pgxaip.FilterRewriter{
			Filter:  parseFilter(`name = "Alice"`, filtering.DeclareIdent("name", filtering.TypeString)),
			Columns: map[string]string{"name": "name"},
		}
		sql, _, err := r.RewriteQuery(ctx, nil, `WHERE TRUE /* AND query.filter */`, []any{r})
		Expect(err).NotTo(HaveOccurred())
		Expect(sql).To(Equal(`WHERE TRUE AND ("name" = $1)`))
	})

	It("removes the sentinel entirely when the filter is empty", func() {
		r := &pgxaip.FilterRewriter{}
		sql, args, err := r.RewriteQuery(ctx, nil,
			`WHERE /* query.filter AND */ TRUE`, []any{r, "x"})
		Expect(err).NotTo(HaveOccurred())
		Expect(sql).To(Equal(`WHERE  TRUE`))
		Expect(args).To(Equal([]any{"x"}))
	})

	It("preserves and extends existing positional args", func() {
		r := &pgxaip.FilterRewriter{
			Filter:  parseFilter(`age > 30`, filtering.DeclareIdent("age", filtering.TypeInt)),
			Columns: map[string]string{"age": "age"},
		}
		sql, args, err := r.RewriteQuery(ctx, nil,
			`WHERE /* query.filter AND */ TRUE AND other = $1`, []any{r, "existing"})
		Expect(err).NotTo(HaveOccurred())
		Expect(sql).To(ContainSubstring(`$2`))
		Expect(args).To(Equal([]any{"existing", int64(30)}))
	})

	It("fails closed when a filter field is not in Columns", func() {
		r := &pgxaip.FilterRewriter{
			Filter:  parseFilter(`name = "Alice"`, filtering.DeclareIdent("name", filtering.TypeString)),
			Columns: map[string]string{"other": "other"},
		}
		_, _, err := r.RewriteQuery(ctx, nil, `WHERE /* query.filter AND */ TRUE`, []any{r})
		Expect(err).To(MatchError(ContainSubstring(`unknown field "name"`)))
	})

	It("fails closed when Columns is nil", func() {
		r := &pgxaip.FilterRewriter{
			Filter: parseFilter(`name = "Alice"`, filtering.DeclareIdent("name", filtering.TypeString)),
		}
		_, _, err := r.RewriteQuery(ctx, nil, `WHERE /* query.filter AND */ TRUE`, []any{r})
		Expect(err).To(MatchError(ContainSubstring(`unknown field "name"`)))
	})
})
