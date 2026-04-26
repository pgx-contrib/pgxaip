package pgxaip_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pgx-contrib/pgxaip"
	"go.einride.tech/aip/filtering"
	"go.einride.tech/aip/pagination"
)

var _ = Describe("Query.Rewrite", func() {
	It("returns all zeroes for an empty query", func() {
		q := pgxaip.Query{}
		where, order, args, err := q.Rewrite()
		Expect(err).NotTo(HaveOccurred())
		Expect(where).To(BeEmpty())
		Expect(order).To(BeEmpty())
		Expect(args).To(BeEmpty())
	})

	It("combines filter + order + cursor with correct placeholder numbering", func() {
		q := pgxaip.Query{
			Filter: parseFilter(`name = "Alice"`,
				filtering.DeclareIdent("name", filtering.TypeString)),
			OrderBy:   parseOrderBy("name desc, id"),
			PageToken: pagination.PageToken{Cursor: []any{"Bob", "uuid-7"}},
			Columns:   map[string]string{"name": "name", "id": "id"},
		}
		where, order, args, err := q.Rewrite()
		Expect(err).NotTo(HaveOccurred())
		Expect(where).To(Equal(`("name" = $1) AND (("name" < $2) OR ("name" = $2 AND "id" > $3))`))
		Expect(order).To(Equal(`"name" DESC, "id" ASC`))
		Expect(args).To(Equal([]any{"Alice", "Bob", "uuid-7"}))
	})

	It("ignores PageToken.Offset", func() {
		q := pgxaip.Query{
			OrderBy:   parseOrderBy("id"),
			PageToken: pagination.PageToken{Offset: 42},
			Columns:   map[string]string{"id": "id"},
		}
		where, order, args, err := q.Rewrite()
		Expect(err).NotTo(HaveOccurred())
		Expect(where).To(BeEmpty())
		Expect(order).To(Equal(`"id" ASC`))
		Expect(args).To(BeEmpty())
	})

	It("uses a single Columns map for both filter and order", func() {
		q := pgxaip.Query{
			Filter: parseFilter(`name = "Alice"`,
				filtering.DeclareIdent("name", filtering.TypeString)),
			OrderBy: parseOrderBy("create_time desc"),
			Columns: map[string]string{
				"name":        "name",
				"create_time": "created_at",
			},
		}
		where, order, args, err := q.Rewrite()
		Expect(err).NotTo(HaveOccurred())
		Expect(where).To(Equal(`"name" = $1`))
		Expect(order).To(Equal(`"created_at" DESC`))
		Expect(args).To(Equal([]any{"Alice"}))
	})
})

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

	It("translates `:` into a LIKE containment check", func() {
		q := pgxaip.Query{
			Filter:  parseFilter(`name:"ali"`, filtering.DeclareIdent("name", filtering.TypeString)),
			Columns: map[string]string{"name": "name"},
		}
		where, _, args, err := q.Rewrite()
		Expect(err).NotTo(HaveOccurred())
		Expect(where).To(Equal(`"name" LIKE '%' || $1 || '%'`))
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

var _ = Describe("Query.Rewrite order_by", func() {
	It("emits a single ASC term", func() {
		q := pgxaip.Query{
			OrderBy: parseOrderBy("name"),
			Columns: map[string]string{"name": "name"},
		}
		_, order, _, err := q.Rewrite()
		Expect(err).NotTo(HaveOccurred())
		Expect(order).To(Equal(`"name" ASC`))
	})

	It("emits mixed directions and multiple fields", func() {
		q := pgxaip.Query{
			OrderBy: parseOrderBy("name desc, age asc"),
			Columns: map[string]string{"name": "name", "age": "age"},
		}
		_, order, _, err := q.Rewrite()
		Expect(err).NotTo(HaveOccurred())
		Expect(order).To(Equal(`"name" DESC, "age" ASC`))
	})

	It("maps AIP paths to their backing DB columns", func() {
		q := pgxaip.Query{
			OrderBy: parseOrderBy("create_time desc"),
			Columns: map[string]string{"create_time": "created_at"},
		}
		_, order, _, err := q.Rewrite()
		Expect(err).NotTo(HaveOccurred())
		Expect(order).To(Equal(`"created_at" DESC`))
	})

	It("sanitizes dotted paths on the mapped column", func() {
		q := pgxaip.Query{
			OrderBy: parseOrderBy("address.city"),
			Columns: map[string]string{"address.city": "addr.city"},
		}
		_, order, _, err := q.Rewrite()
		Expect(err).NotTo(HaveOccurred())
		Expect(order).To(Equal(`"addr"."city" ASC`))
	})

	It("returns empty order when no fields are supplied", func() {
		q := pgxaip.Query{}
		_, order, _, err := q.Rewrite()
		Expect(err).NotTo(HaveOccurred())
		Expect(order).To(BeEmpty())
	})

	It("fails closed when an order field is not in Columns", func() {
		q := pgxaip.Query{
			OrderBy: parseOrderBy("name"),
			Columns: map[string]string{"other": "other"},
		}
		_, _, _, err := q.Rewrite()
		Expect(err).To(MatchError(ContainSubstring(`order: unknown field "name"`)))
	})
})

var _ = Describe("Query.Rewrite cursor", func() {
	It("emits a single-column predicate for ASC ordering", func() {
		q := pgxaip.Query{
			OrderBy:   parseOrderBy("id"),
			PageToken: pagination.PageToken{Cursor: []any{"abc"}},
			Columns:   map[string]string{"id": "id"},
		}
		where, _, args, err := q.Rewrite()
		Expect(err).NotTo(HaveOccurred())
		Expect(where).To(Equal(`(("id" > $1))`))
		Expect(args).To(Equal([]any{"abc"}))
	})

	It("flips comparison direction for DESC columns", func() {
		q := pgxaip.Query{
			OrderBy:   parseOrderBy("name desc"),
			PageToken: pagination.PageToken{Cursor: []any{"Z"}},
			Columns:   map[string]string{"name": "name"},
		}
		where, _, _, err := q.Rewrite()
		Expect(err).NotTo(HaveOccurred())
		Expect(where).To(ContainSubstring(`"name" < $1`))
	})

	It("maps AIP paths to their backing DB columns", func() {
		q := pgxaip.Query{
			OrderBy:   parseOrderBy("create_time desc"),
			PageToken: pagination.PageToken{Cursor: []any{"2025-01-01"}},
			Columns:   map[string]string{"create_time": "created_at"},
		}
		where, _, _, err := q.Rewrite()
		Expect(err).NotTo(HaveOccurred())
		Expect(where).To(ContainSubstring(`"created_at" < $1`))
	})

	It("emits a compound predicate with equality prefixes", func() {
		q := pgxaip.Query{
			OrderBy:   parseOrderBy("name desc, id"),
			PageToken: pagination.PageToken{Cursor: []any{"Alice", "uuid-1"}},
			Columns:   map[string]string{"name": "name", "id": "id"},
		}
		where, _, args, err := q.Rewrite()
		Expect(err).NotTo(HaveOccurred())
		Expect(where).To(Equal(`(("name" < $1) OR ("name" = $1 AND "id" > $2))`))
		Expect(args).To(Equal([]any{"Alice", "uuid-1"}))
	})

	It("numbers cursor placeholders after filter args", func() {
		q := pgxaip.Query{
			Filter:    parseFilter(`name = "Alice"`, filtering.DeclareIdent("name", filtering.TypeString)),
			OrderBy:   parseOrderBy("id"),
			PageToken: pagination.PageToken{Cursor: []any{"x"}},
			Columns:   map[string]string{"name": "name", "id": "id"},
		}
		where, _, args, err := q.Rewrite()
		Expect(err).NotTo(HaveOccurred())
		Expect(where).To(Equal(`("name" = $1) AND (("id" > $2))`))
		Expect(args).To(Equal([]any{"Alice", "x"}))
	})

	It("returns empty where when no cursor is supplied", func() {
		q := pgxaip.Query{
			OrderBy: parseOrderBy("id"),
			Columns: map[string]string{"id": "id"},
		}
		where, order, args, err := q.Rewrite()
		Expect(err).NotTo(HaveOccurred())
		Expect(where).To(BeEmpty())
		Expect(order).To(Equal(`"id" ASC`))
		Expect(args).To(BeEmpty())
	})

	It("errors on length mismatch between OrderBy and Cursor", func() {
		q := pgxaip.Query{
			OrderBy:   parseOrderBy("id"),
			PageToken: pagination.PageToken{Cursor: []any{"x", "y"}},
			Columns:   map[string]string{"id": "id"},
		}
		_, _, _, err := q.Rewrite()
		Expect(err).To(MatchError(ContainSubstring("cursor: expected 1 values for 1 fields, got 2")))
	})

	It("fails closed when a cursor field is not in Columns", func() {
		q := pgxaip.Query{
			OrderBy:   parseOrderBy("name"),
			PageToken: pagination.PageToken{Cursor: []any{"Alice"}},
			Columns:   map[string]string{"other": "other"},
		}
		_, _, _, err := q.Rewrite()
		// The order rewriter runs first and catches this before the cursor path.
		Expect(err).To(MatchError(ContainSubstring(`"name"`)))
	})
})
