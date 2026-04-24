package pgxquery_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pgx-contrib/pgxquery"
	"go.einride.tech/aip/filtering"
)

var _ = Describe("FilterRewriter", func() {
	It("substitutes a simple equality predicate", func() {
		r := &pgxquery.FilterRewriter{
			Filter:  parseFilter(`name = "Alice"`, filtering.DeclareIdent("name", filtering.TypeString)),
			Columns: map[string]string{"name": "name"},
		}
		sql, args, err := r.Substitute(`WHERE /* query.filter AND */ TRUE`, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(sql).To(Equal(`WHERE ("name" = $1) AND TRUE`))
		Expect(args).To(Equal([]any{"Alice"}))
	})

	It("maps AIP paths to their backing DB columns", func() {
		r := &pgxquery.FilterRewriter{
			Filter:  parseFilter(`create_time = "2025-01-01"`, filtering.DeclareIdent("create_time", filtering.TypeString)),
			Columns: map[string]string{"create_time": "created_at"},
		}
		sql, args, err := r.Substitute(`WHERE /* query.filter AND */ TRUE`, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(sql).To(Equal(`WHERE ("created_at" = $1) AND TRUE`))
		Expect(args).To(Equal([]any{"2025-01-01"}))
	})

	It("supports trailing-position sentinel with inside glue", func() {
		r := &pgxquery.FilterRewriter{
			Filter:  parseFilter(`name = "Alice"`, filtering.DeclareIdent("name", filtering.TypeString)),
			Columns: map[string]string{"name": "name"},
		}
		sql, _, err := r.Substitute(`WHERE TRUE /* AND query.filter */`, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(sql).To(Equal(`WHERE TRUE AND ("name" = $1)`))
	})

	It("removes the sentinel entirely when the filter is empty", func() {
		r := &pgxquery.FilterRewriter{}
		sql, args, err := r.Substitute(`WHERE /* query.filter AND */ TRUE`, []any{"x"})
		Expect(err).NotTo(HaveOccurred())
		Expect(sql).To(Equal(`WHERE  TRUE`))
		Expect(args).To(Equal([]any{"x"}))
	})

	It("preserves and extends existing positional args", func() {
		r := &pgxquery.FilterRewriter{
			Filter:  parseFilter(`age > 30`, filtering.DeclareIdent("age", filtering.TypeInt)),
			Columns: map[string]string{"age": "age"},
		}
		sql, args, err := r.Substitute(`WHERE /* query.filter AND */ TRUE AND other = $1`, []any{"existing"})
		Expect(err).NotTo(HaveOccurred())
		Expect(sql).To(ContainSubstring(`$2`))
		Expect(args).To(Equal([]any{"existing", int64(30)}))
	})

	It("fails closed when a filter field is not in Columns", func() {
		r := &pgxquery.FilterRewriter{
			Filter:  parseFilter(`name = "Alice"`, filtering.DeclareIdent("name", filtering.TypeString)),
			Columns: map[string]string{"other": "other"},
		}
		_, _, err := r.Substitute(`WHERE /* query.filter AND */ TRUE`, nil)
		Expect(err).To(MatchError(ContainSubstring(`unknown field "name"`)))
	})

	It("fails closed when Columns is nil", func() {
		r := &pgxquery.FilterRewriter{
			Filter: parseFilter(`name = "Alice"`, filtering.DeclareIdent("name", filtering.TypeString)),
		}
		_, _, err := r.Substitute(`WHERE /* query.filter AND */ TRUE`, nil)
		Expect(err).To(MatchError(ContainSubstring(`unknown field "name"`)))
	})
})
