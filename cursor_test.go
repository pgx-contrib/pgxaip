package pgxaip_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pgx-contrib/pgxaip"
	"go.einride.tech/aip/filtering"
	"go.einride.tech/aip/pagination"
)

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
