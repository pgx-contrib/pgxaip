package pgxaip_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pgx-contrib/pgxaip"
	"go.einride.tech/aip/filtering"
)

var _ = Describe("Query.Rewrite filter", func() {
	It("emits an equality predicate with a bound arg", func() {
		q := pgxaip.Query{
			Filter:  parseFilter(`name = "Alice"`, filtering.DeclareIdent("name", filtering.TypeString)),
			Columns: map[string]string{"name": "name"},
		}
		where, _, args, err := q.Rewrite()
		Expect(err).NotTo(HaveOccurred())
		Expect(where).To(Equal(`"name" = $1`))
		Expect(args).To(Equal([]any{"Alice"}))
	})

	It("maps AIP paths to their backing DB columns", func() {
		q := pgxaip.Query{
			Filter:  parseFilter(`title = "The Go Programming Language"`, filtering.DeclareIdent("title", filtering.TypeString)),
			Columns: map[string]string{"title": "book_title"},
		}
		where, _, args, err := q.Rewrite()
		Expect(err).NotTo(HaveOccurred())
		Expect(where).To(Equal(`"book_title" = $1`))
		Expect(args).To(Equal([]any{"The Go Programming Language"}))
	})

	It("returns empty where when no filter is set", func() {
		q := pgxaip.Query{}
		where, order, args, err := q.Rewrite()
		Expect(err).NotTo(HaveOccurred())
		Expect(where).To(BeEmpty())
		Expect(order).To(BeEmpty())
		Expect(args).To(BeEmpty())
	})

	It("combines AND with parentheses per branch", func() {
		q := pgxaip.Query{
			Filter: parseFilter(`name = "Alice" AND age > 30`,
				filtering.DeclareIdent("name", filtering.TypeString),
				filtering.DeclareIdent("age", filtering.TypeInt)),
			Columns: map[string]string{"name": "name", "age": "age"},
		}
		where, _, args, err := q.Rewrite()
		Expect(err).NotTo(HaveOccurred())
		Expect(where).To(Equal(`("name" = $1 AND "age" > $2)`))
		Expect(args).To(Equal([]any{"Alice", int64(30)}))
	})

	It("combines OR with parentheses per branch", func() {
		q := pgxaip.Query{
			Filter: parseFilter(`name = "Alice" OR name = "Bob"`,
				filtering.DeclareIdent("name", filtering.TypeString)),
			Columns: map[string]string{"name": "name"},
		}
		where, _, args, err := q.Rewrite()
		Expect(err).NotTo(HaveOccurred())
		Expect(where).To(Equal(`("name" = $1 OR "name" = $2)`))
		Expect(args).To(Equal([]any{"Alice", "Bob"}))
	})

	It("wraps NOT in parentheses", func() {
		q := pgxaip.Query{
			Filter:  parseFilter(`NOT name = "Alice"`, filtering.DeclareIdent("name", filtering.TypeString)),
			Columns: map[string]string{"name": "name"},
		}
		where, _, args, err := q.Rewrite()
		Expect(err).NotTo(HaveOccurred())
		Expect(where).To(Equal(`(NOT "name" = $1)`))
		Expect(args).To(Equal([]any{"Alice"}))
	})

	It("translates `:` into an ILIKE containment check", func() {
		q := pgxaip.Query{
			Filter:  parseFilter(`name:"ali"`, filtering.DeclareIdent("name", filtering.TypeString)),
			Columns: map[string]string{"name": "name"},
		}
		where, _, args, err := q.Rewrite()
		Expect(err).NotTo(HaveOccurred())
		Expect(where).To(Equal(`"name" ILIKE '%' || $1 || '%'`))
		Expect(args).To(Equal([]any{"ali"}))
	})

	It("binds timestamp literals as time.Time", func() {
		q := pgxaip.Query{
			Filter: parseFilter(`create_time > timestamp("2025-01-02T03:04:05Z")`,
				filtering.DeclareIdent("create_time", filtering.TypeTimestamp)),
			Columns: map[string]string{"create_time": "created_at"},
		}
		where, _, args, err := q.Rewrite()
		Expect(err).NotTo(HaveOccurred())
		Expect(where).To(Equal(`"created_at" > $1`))
		Expect(args).To(HaveLen(1))
		Expect(args[0]).To(BeAssignableToTypeOf(time.Time{}))
		Expect(args[0].(time.Time)).To(BeTemporally("==", time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)))
	})

	It("binds duration literals as time.Duration", func() {
		q := pgxaip.Query{
			Filter: parseFilter(`timeout > duration("1h30m")`,
				filtering.DeclareIdent("timeout", filtering.TypeDuration)),
			Columns: map[string]string{"timeout": "timeout"},
		}
		where, _, args, err := q.Rewrite()
		Expect(err).NotTo(HaveOccurred())
		Expect(where).To(Equal(`"timeout" > $1`))
		Expect(args).To(Equal([]any{90 * time.Minute}))
	})

	It("folds unary minus on numeric literals", func() {
		q := pgxaip.Query{
			Filter:  parseFilter(`balance > -5`, filtering.DeclareIdent("balance", filtering.TypeInt)),
			Columns: map[string]string{"balance": "balance"},
		}
		where, _, args, err := q.Rewrite()
		Expect(err).NotTo(HaveOccurred())
		Expect(where).To(Equal(`"balance" > $1`))
		Expect(args).To(Equal([]any{int64(-5)}))
	})

	It("supports comparisons between two columns", func() {
		q := pgxaip.Query{
			Filter: parseFilter(`updated > created`,
				filtering.DeclareIdent("updated", filtering.TypeTimestamp),
				filtering.DeclareIdent("created", filtering.TypeTimestamp)),
			Columns: map[string]string{"updated": "updated_at", "created": "created_at"},
		}
		where, _, args, err := q.Rewrite()
		Expect(err).NotTo(HaveOccurred())
		Expect(where).To(Equal(`"updated_at" > "created_at"`))
		Expect(args).To(BeEmpty())
	})

	It("fails closed when a filter field is not in Columns", func() {
		q := pgxaip.Query{
			Filter:  parseFilter(`name = "Alice"`, filtering.DeclareIdent("name", filtering.TypeString)),
			Columns: map[string]string{"other": "other"},
		}
		_, _, _, err := q.Rewrite()
		Expect(err).To(MatchError(ContainSubstring(`unknown field "name"`)))
	})

	It("fails closed when Columns is nil", func() {
		q := pgxaip.Query{
			Filter: parseFilter(`name = "Alice"`, filtering.DeclareIdent("name", filtering.TypeString)),
		}
		_, _, _, err := q.Rewrite()
		Expect(err).To(MatchError(ContainSubstring(`unknown field "name"`)))
	})
})
